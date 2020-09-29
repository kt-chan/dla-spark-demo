

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import com.typesafe.config.ConfigFactory
import java.util.Properties
import java.util.Random
import java.util.Date

object SparkKafkaPub extends App {

  def run(): Unit = {

    val events = 100;
    val rnd = new Random();
    val config = ConfigFactory.load("application.conf").getConfig("com.alibaba-inc.cwchan");
    val kafkaConfig = config.getConfig("Kafka");
    val bootstrapServers = kafkaConfig.getString("bootstrapServers");
    val groupId = kafkaConfig.getString("groupId");
    val topicName = kafkaConfig.getString("topicName");
    
    val props = new Properties()
    props.put("bootstrap.servers", bootstrapServers)
    // props.put("client.id", "KafkaProducer-cwchan")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val kafkaProducer = new KafkaProducer[String, String](props)

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