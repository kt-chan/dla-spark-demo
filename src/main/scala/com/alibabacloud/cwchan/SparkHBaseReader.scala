package com.alibabacloud.cwchan

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.SparkSession

import com.typesafe.config.ConfigFactory

object SparkHBaseReader extends App {

  var sparkSessoin: SparkSession = null;
  var phoenixConnection: String = null;
  var zookeeperQuorum: String = null;

  def loadConfig(): SparkSession = {

    val config = ConfigFactory.load().getConfig("com.alibaba-inc.cwchan");
    val phoenixConfig = config.getConfig("Phoenix");
    val hostname = phoenixConfig.getString("hostname");
    val port = phoenixConfig.getString("port");
    zookeeperQuorum = phoenixConfig.getString("zookeeper");
    phoenixConnection = phoenixConfig.getString("connection");

    println("running HBaseReader for " + phoenixConnection);

    return SparkApp.sparkSessoin
  }

  def run(): Unit = {

    var sparkSession = loadConfig();
    
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum", zookeeperQuorum)
    hbaseConf.set(TableInputFormat.INPUT_TABLE, "USER_TEST:EMP")

    val hBaseRDD = sparkSession.sparkContext.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])

    hBaseRDD.foreach{ case (_ ,result) =>
      val key = Bytes.toString(result.getRow)
      val name = Bytes.toString(result.getValue("PERSONAL".getBytes,"NAME".getBytes))
      val age = Bytes.toString(result.getValue("PROFESSIONAL".getBytes,"NAME".getBytes))
      println("Row key:"+key+"\tPERSONAL.NAME:"+name+"\tPROFESSIONAL.NAME:"+age)
    }
  }

}