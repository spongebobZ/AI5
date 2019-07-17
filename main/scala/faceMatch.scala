import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.elasticsearch.hadoop.cfg.ConfigurationOptions
import org.apache.spark.sql.functions.udf

object faceMatch {
  def main(args: Array[String]): Unit = {
    if (args.length != 5) {
      println(s"expect for five arguments but got ${args.length}")
      sys.exit(1)
    }
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

    val Array(faceTokenBrokers, faceTokenTopic, taskID, dbList, matchThreshold, esHost) = args
    val esIP = esHost.split(":")(0)
    val esPort = esHost.split(":")(1)

    val matchFaceInMultiDB = udf((faceToken: String) => {
      /**
        * faceToken : faceTokens from detect result
        * return : match faces
        */
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
    val df_format = df_faceToken.selectExpr("CAST(value AS STRINIG)")
      .mapPartitions(part => {
        part.map(row => {
          row.getAs[String]("value").split(",")
        })
      }).selectExpr("value[0] AS taskID", "value[1] AS eventTime", "value[2] AS imagePath", "value[3] AS faceTokens")


  }

}
