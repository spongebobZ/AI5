import java.io.FileInputStream
import java.util.Properties

import com.alibaba.fastjson.JSON
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.size
import org.elasticsearch.hadoop.cfg.ConfigurationOptions
import scalaj.http.Http

import scala.collection.mutable.ArrayBuffer

object fuzzySearch {
  def main(args: Array[String]): Unit = {
    if (args.length < 6) {
      println(s"expect for six arguments but got ${args.length}")
      sys.exit(1)
    }
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    val Array(faceTokenBrokers, faceTokenTopic, aiHost, taskID, esHost, conditionString) = args
    val esIP = esHost.split(":")(0)
    val esPort = esHost.split(":")(1)
    val props = new Properties()
    props.load(new FileInputStream("/data/spark-submit-scripts/accessToken.properties"))
    //    props.load(new FileInputStream("src/main/resources/accessToken.properties"))
    val accessToken = props.getProperty("access_token")
    val detectUrl = aiHost + "/detect"

    val conditionStrMap = collection.mutable.Map[String, String]("expression" -> "", "faceShape" -> "", "gender" -> "",
      "glasses" -> "", "emotion" -> "", "race" -> "")

    val conditionNumMapTemp = collection.mutable.Map[String, Int]("ageLow" -> -1, "ageHigh" -> -1, "beautyLow" -> -1, "beautyHigh" -> -1)

    val conditionJson = JSON.parseObject(conditionString)

    for (k <- conditionStrMap.keySet) {
      val tmp = conditionJson.getString(k)
      if (tmp != null) {
        conditionStrMap(k) = tmp
      } else {
        conditionStrMap -= k
      }
    }

    for (k <- conditionNumMapTemp.keySet) {
      val tmp = conditionJson.getInteger(k)
      if (tmp != null) {
        conditionNumMapTemp(k) = tmp.toInt
      } else {
        conditionNumMapTemp -= k
      }
    }

    // init range filter conditions
    val conditionNumMap = new collection.mutable.HashMap[String, (Int, Int)]()
    for (k <- conditionNumMapTemp.keySet) {
      if (conditionNumMapTemp.contains(k)) {
        if (k.contains("Low")) {
          val i = k.lastIndexOf("Low")
          val pre = k.substring(0, i)
          if (conditionNumMapTemp.contains(pre.concat("High"))) {
            conditionNumMap += pre -> (conditionNumMapTemp(k) -> conditionNumMapTemp(pre.concat("High")))
            conditionNumMapTemp -= pre.concat("High")
          } else {
            conditionNumMap += pre -> (conditionNumMapTemp(k) -> 100)
          }
        } else if (k.contains("High")) {
          val i = k.lastIndexOf("High")
          val pre = k.substring(0, i)
          if (conditionNumMapTemp.contains(pre.concat("Low"))) {
            conditionNumMap += pre -> (conditionNumMapTemp(pre.concat("Low")) -> conditionNumMapTemp(k))
            conditionNumMapTemp -= pre.concat("Low")
          } else {
            conditionNumMap += pre -> (0 -> conditionNumMapTemp(k))
          }
        }
      }
    }
    println(s"filter condition: ${conditionStrMap.toString}, ${conditionNumMap.toString}")
    val searchKeyWordsStr = conditionStrMap.keySet
    val searchKeyWordsNum = conditionNumMap.keySet
    val searchKeyWordsString = (searchKeyWordsStr ++ searchKeyWordsNum).mkString(",")

    def fuzzySearchFace(faceTokens: String): Array[String] = {
      /**
        *
        * faceToken : faceTokens from detect result, "," as sep
        * return : array contains faces those matches with search condition, empty array when no any matches
        */
      val faceList = faceTokens.split(",")
      val matchFaces = ArrayBuffer[String]()
      for (face <- faceList) {
        val rsp = Http(detectUrl).param("access_token", accessToken)
          .postData(
            s"""
               {
               "image": "$face",
               "image_type": "FACE_TOKEN",
               "face_field": "$searchKeyWordsString",
               "max_user_num":10
               }
            """
          ).asString
        if (rsp.isSuccess) {
          val result = JSON.parseObject(rsp.body).getJSONObject("result")
          result match {
            case null =>
            case _ =>
              val rsList = result.getJSONArray("face_list").toArray
              var flag = 0
              for (rs <- rsList) {
                // actually it would has one rs only in rsList
                val f = JSON.parseObject(rs.toString)
                for (k <- searchKeyWordsStr if flag == 0) {
                  if (JSON.parseObject(f.getString(k)).getString("type") != conditionStrMap(k)) {
                    flag = 1
                  }
                }
                for (k <- searchKeyWordsNum if flag == 0) {
                  if (f.getInteger(k) < conditionNumMap(k)._1 || f.getInteger(k) > conditionNumMap(k)._2) {
                    flag = 1
                  }
                }
              }
              if (flag == 0) {
                matchFaces += face
              }
          }
        }
      }
      matchFaces.toArray
    }


    val spark = SparkSession.builder.appName(s"fuzzy_search_faces_$taskID")
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

    // apply records to search function and get result Dataframe
    val df_searchResult = df_format.filter($"taskID" === taskID)
      .mapPartitions(part => {
        part.map(row => {
          (row.getAs[String]("taskID"), row.getAs[String]("eventTime")
            , row.getAs[String]("imagePath"), fuzzySearchFace(row.getAs[String]("faceTokens")))
        })
      }).selectExpr("_1 AS taskID", "_2 AS eventTime", "_3 AS imagePath", "_4 AS searchResult")

    val df_searchMatch = df_searchResult.filter(size($"searchResult") > 0)

    val task = df_searchMatch.writeStream
      .format("es")
      .option("checkpointLocation", s"/data/checkpoints/fuzzySearch_$taskID")
      .start(taskID + "/" + "fuzzySearch")

    task.awaitTermination()
    spark.stop()

  }


}
