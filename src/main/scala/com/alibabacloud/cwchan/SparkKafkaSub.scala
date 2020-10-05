package com.alibabacloud.cwchan

import org.apache.spark.sql.SparkSession
import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf
import java.io.File

object SparkKafkaSub {

  var bootstrapServers: String = null;
  var groupId: String = null;
  var topicName: String = null;
  var sparkSessoin: SparkSession = null;

  var kafkaLoginModule: String = null;
  var kafkaUsername: String = null;
  var kafkaPassword: String = null;
  var jassConfigEntity: String = null;

  var jksStorePath: String = null;

  def loadConfig(): SparkSession = {

    val config = ConfigFactory.load().getConfig("com.alibaba-inc.cwchan");
    val kafkaConfig = config.getConfig("Kafka");
    bootstrapServers = kafkaConfig.getString("bootstrapServers");
    groupId = kafkaConfig.getString("groupId");
    topicName = kafkaConfig.getString("topicName");

    val kafkaLoginConfig = config.getConfig("KafkaClientLogin");
    kafkaLoginModule = kafkaLoginConfig.getString("loginModule"); ;
    kafkaUsername = kafkaLoginConfig.getString("username"); ;
    kafkaPassword = kafkaLoginConfig.getString("password"); ;
    jassConfigEntity = "org.apache.kafka.common.security.plain.PlainLoginModule required \n" +
      "        username=" + "\"" + kafkaUsername + "\" \n" +
      "        password=" + "\"" + kafkaPassword + "\";"

    jksStorePath = Utility.getJKSFile();

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

    println("JKS Store Path: " + jksStorePath);
    println("running kafka subscriber for " + topicName);

    val df = sparkSessoin
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("subscribe", topicName)
      .option("group.id", groupId)
      .option("kafka.ssl.truststore.location", jksStorePath)
      .option("kafka.ssl.truststore.password", "KafkaOnsClient")
      .option("kafka.sasl.jaas.config", jassConfigEntity)
      .option("kafka.sasl.mechanism", "PLAIN")
      .option("kafka.security.protocol", "SASL_SSL")
      .load()

    val query = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .writeStream
      .outputMode("append")
      .format("console")
      .start()

    query.awaitTermination()

  }

}