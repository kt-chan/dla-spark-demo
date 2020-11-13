package com.alibabacloud.cwchan

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkApp {

  var sparkSession: SparkSession = null;

  def main(args: Array[String]): Unit = {

    if (args.length != 2) {
      System.err.println("please start with parameter [debug|run] [pub|sub]");
      System.exit(1);
    }

    val debugMode = args(0).toString();

    val sparkConf: SparkConf = new SparkConf();
    sparkConf.setAppName("SparkKafkaSub");
    
    if (debugMode.equalsIgnoreCase("debug")) {
      sparkConf.setMaster("local[4]");
    }

    //SparkContext
    sparkSession = SparkSession
      .builder()
      .enableHiveSupport()
      .config(sparkConf)
      .getOrCreate();

    run(args(1));
  }

  def run(cmd: String): Unit = {

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

    if (cmd.equalsIgnoreCase("sub")) threadSub.start
    if (cmd.equalsIgnoreCase("pub")) threadPub.start

  }
}