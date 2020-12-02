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
  
  def testJDBC(output: DataFrame): Unit = {
    import java.sql.{ DriverManager, SQLException }
    import java.util.Properties
    val driver = "org.apache.phoenix.queryserver.client.Driver"
    val url = "jdbc:phoenix:thin:url=http://hb-3ns6mh3a0074kbkcw-proxy-phoenix.hbase.rds.aliyuncs.com:8765;serialization=PROTOBUF"
    val info = new Properties()
//    info.put("user", "xxxx") //表示用户名是root
//    info.put("password", "xxxx") //表示密码是hadoop
    try {
      Class.forName(driver)
    } catch {
      case e: ClassNotFoundException => e.printStackTrace
    }
    val conn = DriverManager.getConnection(url, info)
    val stmt = conn.createStatement
    try {
      stmt.execute("drop table if exists test")
      stmt.execute("create table test(c1 integer primary key, c2 integer)")
      stmt.execute("upsert into test(c1,c2) values(1,1)")
      stmt.execute("upsert into test(c1,c2) values(2,2)")
      val rs = stmt.executeQuery("select * from test limit 1000")
      while (rs.next()) {
        println(rs.getString(1) + " | " +
          rs.getString(2))
      }
      stmt.execute("drop table if exists test")
    } catch {
      case e: SQLException => e.printStackTrace()
    } finally {
      if (null != stmt) {
        stmt.close()
      }
      if (null != conn) {
        conn.close()
      }
    }
  }
}