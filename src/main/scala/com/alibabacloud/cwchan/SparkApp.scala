package com.alibabacloud.cwchan

import com.alibabacloud.cwchan.SparkKafkaSub
import com.alibabacloud.cwchan.SparkKafkaPub

object SparkApp {

  def main(args: Array[String]): Unit = {

    val threadPub = new Thread {
      override def run {
        // your custom behavior here
        SparkKafkaPub.run();
      }
    }

    val threadSub = new Thread {
      override def run {
        // your custom behavior here
        SparkKafkaSub.run();
      }
    }

    threadSub.start
    Thread.sleep(5000)
    threadPub.start

  }
}