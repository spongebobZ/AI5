package online

import java.io.FileInputStream
import java.util.Properties

import com.alibaba.fastjson.JSON
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.size
import org.elasticsearch.hadoop.cfg.ConfigurationOptions
import scalaj.http.Http

import scala.collection.mutable.ArrayBuffer

object findStranger {
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
    props.load(new FileInputStream("/data/spark-submit-scripts/accessToken.properties"))
//    props.load(new FileInputStream("src/main/resources/accessToken.properties"))
    val accessToken = props.getProperty("access_token")
    val matchUrl = aiHost + "/search"

    def matchFaceNotInMultiDB(faceTokens: String): Array[String] = {
      /**
        * faceToken : faceTokens from detect result, "," as sep
        * return : not match faces list
        */
      val faceList = faceTokens.split(",")
      val strangerList = ArrayBuffer[String]()
      for (face <- faceList) {
        val rsp = Http(matchUrl).param("access_token", accessToken)
          .postData(
            s"""
               {
               "image": "$face",
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
            case null => strangerList += face
            case _ =>
          }
        }
      }

      strangerList.toArray
    }

    val spark = SparkSession.builder.appName(s"find_stranger_$taskID")
      .config(ConfigurationOptions.ES_NODES, esIP)
      .config(ConfigurationOptions.ES_PORT, esPort)
      .config(ConfigurationOptions.ES_NODES_WAN_ONLY, "true")
      .getOrCreate()


    // consume kafka faceToken topic messages which struct like ("value"->taskID,eventTime,imagePath,faceTokens)
    val df_faceToken = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", faceTokenBrokers)
      .option("subscribe", faceTokenTopic)
      .option("startingOffsets", "latest")
      .option("kafka.Consumer.pullTimeoutMs", 3000L)
      .load()

    // split string value with ",", get taskID,eventTime,imagePath,faceTokens info
    import spark.implicits._
    val df_format = df_faceToken.selectExpr("CAST(value AS string)")
      .mapPartitions(part => {
        part.map(
          row => row.getAs[String]("value").split(",", 4)
        )
      })
      .filter(size($"value") > 3)
      .selectExpr("value[0] AS taskID", "value[1] AS eventTime", "value[2] AS imagePath", "value[3] AS faceTokens")

    // apply records to match function and get result Dataframe
    val df_matchResult = df_format.filter($"taskID" === taskID)
      .mapPartitions(part => {
        part.map(row => {
          (row.getAs[String]("taskID"), row.getAs[String]("eventTime")
            , row.getAs[String]("imagePath"), matchFaceNotInMultiDB(row.getAs[String]("faceTokens")))
        })
      })
      .selectExpr("_1 AS taskID", "_2 AS eventTime", "_3 AS imagePath", "_4 AS matchResult")

    val df_stranger = df_matchResult.filter(size($"matchResult") > 0)

    val task = df_stranger.writeStream
      .format("es")
      .option("checkpointLocation", s"/data/checkpoints/findStranger_$taskID")
      .start(taskID + "/" + "findStranger")

    task.awaitTermination()
    spark.stop()


  }

}
