import java.io.FileInputStream
import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{concat_ws, size, udf}
import scalaj.http.Http

object faceDetect {

  def main(args: Array[String]): Unit = {
    if (args.length != 4) {
      println(s"expect for four arguments but got ${args.length}")
      sys.exit(1)
    }
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    val Array(odsBrokers, odsTopic, httpHost, aiHost, dwBrokers, dwTopic) = args
    val uri = "http://" + httpHost
    val props = new Properties()
    props.load(new FileInputStream("src/main/resources/accessToken.properties"))
    val accessToken = props.getProperty("access_token")
    val downloadHttpImage = udf((imagePath: String) => {
      // imagePath should like "images/000001/image1.jpg"
      val url = uri + "/" + imagePath
      Http(url).asBytes.body
    })

    val detectUrl = "http://" + aiHost + "/detect"
    val getFaceToken = udf((imageStream: Array[Byte]) => {
      val rsp = Http(detectUrl).param("access_token", accessToken)
        .postData(s"""{"image":$imageStream,"image_type":"BASE64","max_face_num":10}""").asString
    })

    val spark = SparkSession.builder().appName("save ods images").master("local[*]").getOrCreate()
    // 消费flume监听视频转图片输出目录发到kafka odsTopic的数据,message结构中key指定了taskID，value为图片生成时间和图片目录，value用逗号分隔
    val df_ods = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", odsBrokers)
      .option("subscribe", odsTopic)
      .option("startingOffsets", "latest")
      .load()

    // 把df_ods的key取出，以及把value切割开，最终得到taskID、timestamp、imagePath三个字段的dataFrame，其中过滤掉字段信息不完整的记录
    import spark.implicits._
    val df_format = df_ods.selectExpr("CAST(key AS STRING) AS taskID", "CAST(value AS STRING) AS value")
      .select(concat_ws(",", $"taskID", $"value").alias("value"))
      .mapPartitions(part => {
        part.map(row => {
          row.getAs[String]("value").split(",")
        })
      })
      .filter(size($"value") === 3)
      .selectExpr("value[0] AS taskID", "value[1] AS timestamp", "value[2] AS imagePath")

    val df_faceToken = df_format

    val task = df_format.writeStream
      .format("console")
      .option("truncate", "false")
      .start()

    task.awaitTermination()
    spark.stop()
  }
}