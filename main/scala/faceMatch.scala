import java.io.FileInputStream
import java.util.Properties

import com.alibaba.fastjson.JSON
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.size
import org.elasticsearch.hadoop.cfg.ConfigurationOptions
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

    def matchFaceInMultiDB(faceTokens: String): (scala.collection.mutable.Map[String, Array[Map[String, String]]], Int) = {
      /**
        * faceToken : faceTokens from detect result, "," as sep
        * return : match faces and mark, mark equals 0 means image not match any face, 1 means match some faces, like
        */
      var mark = 0
      val faceList = faceTokens.split(",")
      val resultMap = scala.collection.mutable.Map[String, Array[Map[String, String]]]()
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
            case null => resultMap(face) = new Array[Map[String, String]](0)
            case _ =>
              val matches = result.getJSONArray("user_list").toArray
              val tmpArray = new Array[Map[String, String]](matches.length)
              for (i <- matches.indices) {
                val tmpMatch = JSON.parseObject(matches(i).toString)
                tmpArray(i) = Map("group_id" -> tmpMatch.getString("group_id"),
                  "user_id" -> tmpMatch.getString("user_id"),
                  "user_info" -> tmpMatch.getString("user_info"),
                  "score" -> tmpMatch.getString("score"))
              }
              resultMap(face) = tmpArray
              mark = 1
          }
        } else {
          resultMap(face) = new Array[Map[String, String]](0)
        }
      }
      resultMap -> mark
    }


    val spark = SparkSession.builder().appName("match_faces_in_aimDB").master("local[*]")
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
      .option("kafkaConsumer.pollTimeoutMs", 3000L)
      .load()

    // split string value with ",", get taskID,eventTime,imagePath,faceTokens info
    import spark.implicits._
    val df_format = df_faceToken.selectExpr("CAST(value AS STRING)")
      .mapPartitions(part => {
        part.map(row => {
          row.getAs[String]("value").split(",", 4)
        })
      })
      .filter(size($"value") > 3)
      .selectExpr("value[0] AS taskID", "value[1] AS eventTime", "value[2] AS imagePath", "value[3] AS faceTokens")

    // map matchFace function with column "faceTokens"
    val df_matchResult = df_format.filter($"taskID" === taskID)
      .mapPartitions(part => {
        part.map(row => {
          (row.getAs[String]("taskID"), row.getAs[String]("eventTime"),
            row.getAs[String]("imagePath"), matchFaceInMultiDB(row.getAs[String]("faceTokens")))
        })
      }).selectExpr("_1 AS taskID", "_2 AS eventTime", "_3 AS imagePath", "_4._1 AS matches", "_4._2 AS mark")

    // filter record that had matched some face in DB
    val df_matchFace = df_matchResult.filter($"mark" === 1)

    // sink final Dataframe to es
    val task = df_matchFace.writeStream
      .format("es")
      .option("checkpointLocation", "hdfs://master:9898/checkpoints/AI5/faceMatch")
      .start(taskID + "/" + "matchResult")

    task.awaitTermination()
    spark.stop()
  }
}