package com.alibabacloud.cwchan

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import com.mongodb.spark.config.ReadConfig
import com.typesafe.config.ConfigFactory

object SparkApp {

  var sparkSessoin: SparkSession = null;
  val config = ConfigFactory.load().getConfig("com.alibaba-inc.cwchan");
  
  {
    val appConfig = config.getConfig("App");
    val debugMode = appConfig.getBoolean("debugMode");

    val sparkConf: SparkConf = new SparkConf();
    sparkConf.setAppName("SparkKafkaSub");
    if (debugMode) {
      sparkConf.setMaster("local[4]");
    }

    //SparkContext
    sparkSessoin = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate();
  }

  def loadMongo(): ReadConfig = {
    
    val mongoconfig = config.getConfig("MongoDB");
    val uri = mongoconfig.getString("uri");
    val db = mongoconfig.getString("db");
    val collection = mongoconfig.getString("collection");
    
    val readConfig = ReadConfig(
      Map(
        "uri" -> uri,
        "database" -> db,
        "collection" -> collection))

    return readConfig;
  }

  def main(args: Array[String]): Unit = {

    val threadPub = new Thread {
      override def run {
        // your custom behavior here
        com.alibabacloud.cwchan.SparkKafkaPub.run();
      }
    }

    val threadSub = new Thread {
      override def run {
        // your custom behavior here
        com.alibabacloud.cwchan.SparkKafkaSub.run();
      }
    }

    if (args == null || args.length == 0) {
      threadSub.start
      Thread.sleep(5000)
      threadPub.start
    }

    if (args.length == 1) {
      if (args(0).equalsIgnoreCase("sub")) threadSub.start
      if (args(0).equalsIgnoreCase("pub")) threadPub.start
    }
  }
}