package com.alibabacloud.cwchan

import java.text.SimpleDateFormat

import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.client.Table
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession

import com.typesafe.config.ConfigFactory

object SparkHBaseWriter {

  var address: String = null;
  var zkport: String = null;
  var driver: String = null;

  def open(): Unit = {
    address = ConfigFactory.load().getConfig("com.alibaba-inc.cwchan").getConfig("HBase").getString("zookeeper")
    zkport = ConfigFactory.load().getConfig("com.alibaba-inc.cwchan").getConfig("HBase").getString("zkport")
    driver = "org.apache.phoenix.spark"
  }

  def close(): Unit = {
  }

  def process(record: DataFrame): Unit = {
    record.write
      .format(driver)
      .mode(SaveMode.Overwrite)
      .option("zkUrl", address.split(",")(0) + ":" + zkport)
      .option("table", "USER_TEST.STOCK")
      .save()
  }
}