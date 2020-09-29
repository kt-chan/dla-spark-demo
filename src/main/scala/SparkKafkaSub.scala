

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.{ Encoder, Encoders }

object SparkKafkaSub {

  var bootstrapServers: String = null;
  var groupId: String = null;
  var topicName: String = null;
  var sparkSessoin: SparkSession = null;

  def loadConfig(): SparkSession = {
    val config = ConfigFactory.load("application.conf").getConfig("com.alibaba-inc.cwchan");
    val kafkaConfig = config.getConfig("Kafka");
    bootstrapServers = kafkaConfig.getString("bootstrapServers");
    groupId = kafkaConfig.getString("groupId");
    topicName = kafkaConfig.getString("topicName");

    val sparkConf: SparkConf = new SparkConf()
      .setAppName("SparkKafkaSub")
      .setMaster("local[4]")

    //SparkContext
    sparkSessoin = SparkSession
      .builder()
      .config(sparkConf)
      .master("local[4]")
      .getOrCreate()

    return sparkSessoin
  }

  def run(): Unit = {

    sparkSessoin = loadConfig()

    println("running kafka suscriber for " + topicName);

    val df = sparkSessoin
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("subscribe", topicName)
      .load()

    val query = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .writeStream
      .outputMode("append")
      .format("console")
      .start()

    query.awaitTermination()

  }

}