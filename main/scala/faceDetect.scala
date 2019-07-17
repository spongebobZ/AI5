import java.io.FileInputStream
import java.util.Properties

import com.alibaba.fastjson.JSON
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{concat_ws, size, udf}
import scalaj.http.{Base64, Http}

object faceDetect {

  def main(args: Array[String]): Unit = {
    if (args.length != 6) {
      println(s"expect for six arguments but got ${args.length}")
      sys.exit(1)
    }
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    val Array(odsBrokers, odsTopic, httpHost, aiHost, dwBrokers, dwTopic) = args
    val uri = "http://" + httpHost
    val props = new Properties()
    props.load(new FileInputStream("src/main/resources/accessToken.properties"))
    val accessToken = props.getProperty("access_token")
    val detectUrl = aiHost + "/detect"

    val downloadHttpImage = udf((imagePath: String) => {
      /**
        * imagePath : relative path of image file under http file server root path,
        * eg "images/000001/image1.jpg", "images" is under "/var/www/html/"
        * return : Array[Byte] type of image file content, if download failed it would be an empty array
        */
      val url = uri + "/" + imagePath
      val rsp = Http(url).asBytes
      if (rsp.isSuccess) {
        rsp.body
      } else {
        println(s"download image failed:$imagePath,${rsp.body.toString}")
        new Array[Byte](0)
      }
    })

    val getFaceToken = udf((imageStream: Array[Byte]) => {
      /**
        * imageStream : Array[Byte] type of image file content
        * return : faceToken Array, if got no face it would be an empty Array
        */
      val imageStreamString = Base64.encode(imageStream).mkString("")
      val rsp = Http(detectUrl).param("access_token", accessToken)
        .postData(s"""{"image":"$imageStreamString","image_type":"BASE64","max_face_num":10}""").asString
      val result = JSON.parseObject(rsp.body).getString("result")
      if (result != null) {
        val faces = JSON.parseObject(result)
        val faceResult = new Array[String](faces.getIntValue("face_num"))
        val faceList = JSON.parseArray(faces.getString("face_list")).toArray()
        for (i <- faceList.indices) {
          faceResult(i) = JSON.parseObject(faceList(i).toString).getString("face_token")
        }
        faceResult
      } else {
        new Array[String](0)
      }
    })

    val spark = SparkSession.builder().appName("detect_face_in_video").master("local[*]").getOrCreate()
    // consume kafka ods topic messages which struct like ("key"->taskID,"value"->eventTime,imagePath)
    val df_ods = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", odsBrokers)
      .option("subscribe", odsTopic)
      .option("startingOffsets", "latest")
      .option("kafkaConsumer.pollTimeoutMs", 3000L)
      .load()

    // get taskID from "key" and get "eventTime,imagePath" from "value" then join them as an dataFrame, need to filter incomplete rows
    import spark.implicits._
    val df_format = df_ods.selectExpr("CAST(key AS STRING) AS taskID", "CAST(value AS STRING) AS value")
      .select(concat_ws(",", $"taskID", $"value").alias("value"))
      .mapPartitions(part => {
        part.map(row => {
          row.getAs[String]("value").split(",")
        })
      })
      .filter(size($"value") === 3)
      .selectExpr("value[0] AS taskID", "value[1] AS eventTime", "value[2] AS imagePath")

    // use imagePath to download image file from http file server and then use file to detect faceToken,
    // result dataFrame should be "taskID,eventTime,imagePath,faceTokens"
    val df_faceToken = df_format.select($"taskID", $"eventTime", $"imagePath", downloadHttpImage($"imagePath").alias("imageByte"))
      .select($"taskID", $"eventTime", $"imagePath", getFaceToken($"imageByte").alias("faceTokens"))
      .select(concat_ws(",", $"taskID", $"eventTime", $"imagePath", $"faceTokens").alias("value"))

    val task = df_faceToken.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", dwBrokers)

      .option("topic", dwTopic)
      .option("checkpointLocation", "src/main/checkpoint/faceDetect")
      .start()

    task.awaitTermination()
    spark.stop()
  }
}