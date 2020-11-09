package com.alibabacloud.cwchan

import org.apache.spark.sql.SparkSession

import com.typesafe.config.ConfigFactory

object SparkKafkaSub {

  var bootstrapServers: String = null;
  var groupId: String = null;
  var topicName: String = null;

  var kafkaLoginModule: String = null;
  var kafkaUsername: String = null;
  var kafkaPassword: String = null;
  var jassConfigEntity: String = null;

  def loadConfig(): SparkSession = {
    val config = ConfigFactory.load().getConfig("com.alibaba-inc.cwchan");
    val kafkaConfig = config.getConfig("Kafka");
    bootstrapServers = kafkaConfig.getString("bootstrapServers");
    groupId = kafkaConfig.getString("groupId");
    topicName = kafkaConfig.getString("topicName");

    println("running kafka subscriber for " + topicName);

    return SparkApp.sparkSessoin;
  }

  def run(): Unit = {

    val sparkSessoin = loadConfig();

    val df = sparkSessoin
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("subscribe", topicName)
      .option("group.id", groupId)
      .load();

    //    val query = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    //      .writeStream
    //      .outputMode("append")
    //      .format("console")
    //      .start()

    val query = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .writeStream.foreach(new SparkHBaseWriter)
      .start()
      .awaitTermination();

  }

}