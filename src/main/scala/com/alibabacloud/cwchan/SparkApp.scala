package com.alibabacloud.cwchan

import java.io.File
import com.typesafe.config.ConfigFactory

object SparkApp {

  
  def main(args: Array[String]): Unit = {

    val config = ConfigFactory.load().getConfig("com.alibaba-inc.cwchan");
    val appConfig = config.getConfig("App");
    val debugMode = appConfig.getBoolean("debugMode");
    
    val threadPub = new Thread {
      override def run {
        // your custom behavior here
        com.alibabacloud.cwchan.SparkKafkaPub.run(debugMode);
      }
    }

    val threadSub = new Thread {
      override def run {
        // your custom behavior here
        com.alibabacloud.cwchan.SparkKafkaSub.run(debugMode);
      }
    }

    if (args == null || args.length == 0) {
      threadSub.start
      Thread.sleep(5000)
      threadPub.start
    }

    if (args.length == 1) {
      if (args(0).equalsIgnoreCase("sub")) threadSub.start
      if (args(0).equalsIgnoreCase("pub"))threadPub.start
    }
  }
}