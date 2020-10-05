

package com.alibabacloud.cwchan

import java.util.Date
import java.util.Properties
import java.util.Random

import scala.Range

import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.config.SslConfigs

import com.typesafe.config.ConfigFactory

object SparkKafkaPub extends App {


  def run(): Unit = {

    val events = 100;
    val rnd = new Random();
    val config = ConfigFactory.load().getConfig("com.alibaba-inc.cwchan");
    val kafkaConfig = config.getConfig("Kafka");
    val bootstrapServers = kafkaConfig.getString("bootstrapServers");
    val groupId = kafkaConfig.getString("groupId");
    val topicName = kafkaConfig.getString("topicName");

    val kafkaLoginConfig = config.getConfig("KafkaClientLogin");
    val kafkaLoginModule = kafkaLoginConfig.getString("loginModule"); ;
    val kafkaUsername = kafkaLoginConfig.getString("username"); ;
    val kafkaPassword = kafkaLoginConfig.getString("password"); ;

    val jksStorePath = Utility.getJKSFile();

    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, jksStorePath)
    props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "KafkaOnsClient");
    props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
    props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
    props.put(SaslConfigs.SASL_JAAS_CONFIG, "org.apache.kafka.common.security.plain.PlainLoginModule required \n" +
      "        username=" + "\"" + kafkaUsername + "\" \n" +
      "        password=" + "\"" + kafkaPassword + "\";");
    props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "30000");
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

    val kafkaProducer = new KafkaProducer[String, String](props)
    println("JKS Store Path: " + jksStorePath);
    println("running kafka publisher for " + topicName);

    for (nEvents <- Range(0, events)) {
      val runtime = new Date().getTime()
      val ip = "192.168.2." + rnd.nextInt(255)
      val msg = runtime + "," + nEvents + ",www.helloworld.com," + ip
      val data = new ProducerRecord[String, String](topicName, ip, msg)

      // println("sending to topic: " + data.topic() + "; data: " + data.value())
      kafkaProducer.send(data)

      Thread.sleep(1000)

    }

  }

}