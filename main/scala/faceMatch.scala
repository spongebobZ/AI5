import java.io.FileInputStream
import java.util.Properties

import com.alibaba.fastjson.JSON
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.elasticsearch.hadoop.cfg.ConfigurationOptions
import org.apache.spark.sql.functions.{udf, size}
import scalaj.http.Http

object faceMatch {
  def main(args: Array[String]): Unit = {
    if (args.length != 7) {
      println(s"expect for seven arguments but got ${args.length}")
      sys.exit(1)
    }
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

    val Array(faceTokenBrokers, faceTokenTopic, aiHost, taskID, dbList, matchThreshold, esHost) = args
    val esIP = esHost.split(":")(0)
    val esPort = esHost.split(":")(1)
    val props = new Properties()
    props.load(new FileInputStream("src/main/resources/accessToken.properties"))
    val accessToken = props.getProperty("access_token")
    val matchUrl = aiHost + "/search"
    val matchFaceInMultiDB = udf((faceTokens: String) => {
      /**
        * faceToken : faceTokens from detect result, "," as sep
        * return : match faces
        */
      val faceList = faceTokens.split(",")
      val resultList = new Array[Map[String, Array[Map[String, String]]]](faceList.length)
      for (i <- faceList.indices) {
        val rsp = Http(matchUrl).param("access_token", accessToken)
          .postData(
            s"""
               {
               "image": "${faceList(i)}",
               "image_type": "FACE_TOKEN",
               "group_id_list": "$dbList",
               "match_threshold":${matchThreshold.toInt},
               "max_user_num":10,
               "quality_control": "NONE",
               "liveness_control": "NONE"
               }
            """
          ).asString
        if (rsp.isSuccess) {
          val result = JSON.parseObject(rsp.body).getJSONObject("result")
          result match {
            case null => resultList(i) = Map(faceList(i) -> new Array[Map[String, String]](0))
            case _ =>
              val matches = result.getJSONArray("user_list").toArray
              val tmpArray = new Array[Map[String, String]](matches.length)
              for (j <- matches.indices) {
                val tmpMatch = JSON.parseObject(matches(j).toString)
                tmpArray(j) = Map("group_id" -> tmpMatch.getString("group_id"),
                  "user_id" -> tmpMatch.getString("user_id"),
                  "user_info" -> tmpMatch.getString("user_info"),
                  "score" -> tmpMatch.getString("score"))
              }
          }
        } else {
          resultList(i) = Map(faceList(i) -> new Array[Map[String, String]](0))
        }
      }
      resultList
    })

    val spark = SparkSession.builder().appName("match_faces_in_aimDB").master("local[*]")
      .config(ConfigurationOptions.ES_NODES, esIP)
      .config(ConfigurationOptions.ES_PORT, esPort)
      .getOrCreate()

    val df_faceToken = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", faceTokenBrokers)
      .option("subscribe", faceTokenTopic)
      .option("startingOffsets", "latest")
      .option("kafkaConsumer.pollTimeoutMs", 3000L)
      .load()

    import spark.implicits._
    val df_format = df_faceToken.selectExpr("CAST(value AS STRING)")
      .mapPartitions(part => {
        part.map(row => {
          row.getAs[String]("value").split(",", 4)
        })
      })
      .filter(size($"value") > 3)
      .selectExpr("value[0] AS taskID", "value[1] AS eventTime", "value[2] AS imagePath", "value[3] AS faceTokens")

    val df_matchResult = df_format.filter($"taskID" === taskID)
      .select($"taskID", $"eventTime", $"imagePath", matchFaceInMultiDB($"faceTokens").alias("matchResult"))

    val task = df_matchResult.writeStream
      .format("console")
      .option("truncate", "false")
      .start()

    task.awaitTermination()
    spark.stop()
  }
}