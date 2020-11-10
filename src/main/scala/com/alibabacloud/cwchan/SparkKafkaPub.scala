

package com.alibabacloud.cwchan

import java.util.Date
import java.util.Properties
import java.util.Random

import scala.Range

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord

import com.typesafe.config.ConfigFactory

object SparkKafkaPub extends App {

  var topicName: String = null;

  def loadConfig(): Properties = {
    val config = ConfigFactory.load().getConfig("com.alibaba-inc.cwchan");
    val kafkaConfig = config.getConfig("Kafka");
    val bootstrapServers = kafkaConfig.getString("bootstrapServers");
    val groupId = kafkaConfig.getString("groupId");
    topicName = kafkaConfig.getString("topicName");

    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "30000");
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

    println("running kafka publisher for " + topicName);

    return props;
  }

  def run(): Unit = {

    val props = loadConfig();

    val events = 1000;
    val rnd = new Random();

    val kafkaProducer = new KafkaProducer[String, String](props)

    for (nEvents <- Range(0, events)) {
      val runtime = new Date().getTime()
      val ip = "192.168.2." + rnd.nextInt(255)
      val msg =  SKUs.HSIStocks(rnd.nextInt(SKUs.HSIStocks.length-1)) + "," + runtime;
      val data = new ProducerRecord[String, String](topicName, ip, msg)

      // println("sending to topic: " + data.topic() + "; data: " + data.value())
      kafkaProducer.send(data)

      Thread.sleep(1000)

    }

  }

}