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
  var jassConfigEntity:String = null;

  def loadConfig(): SparkSession = {
    
//    if (null == System.getProperty("java.security.auth.login.config")) {
//      System.setProperty("java.security.auth.login.config", "./src/main/resources/kafka_client_jaas.conf");
//    }

    val config = ConfigFactory.parseFile(new File("./src/main/resources/application.conf")).getConfig("com.alibaba-inc.cwchan");
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

    println("running kafka subscriber for " + topicName);

    val df = sparkSessoin
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("subscribe", topicName)
      .option("group.id", groupId)
      .option("kafka.ssl.truststore.location", "./libs/kafka.client.truststore.jks")
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