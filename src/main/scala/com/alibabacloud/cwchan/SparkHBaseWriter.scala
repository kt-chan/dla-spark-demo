package com.alibabacloud.cwchan

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.sql.SparkSession

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.mapreduce.Job

object SparkHBaseWriter extends App {

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

    println("running HBaseWriter for " + phoenixConnection);

    return SparkApp.sparkSessoin
  }

  def run(): Unit = {

    var sparkSession = loadConfig();

    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum", zookeeperQuorum)
    hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, "USER_TEST:EMP")

    val job = new Job(hbaseConf)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])

    val indataRDD = sparkSession.sparkContext.makeRDD(Array("2,chiwai.chan,alibaba cloud", "3,jonathan.peng,alibaba cloud"))

    val rdd = indataRDD.map(_.split(',')).map { arr =>
      val put = new Put(Bytes.toBytes(arr(0)))
      put.addColumn(Bytes.toBytes("PERSONAL"), Bytes.toBytes("NAME"), Bytes.toBytes(arr(1)))
      put.addColumn(Bytes.toBytes("PROFESSIONAL"), Bytes.toBytes("NAME"), Bytes.toBytes(arr(2)))
      (new ImmutableBytesWritable, put)
    }
    rdd.saveAsNewAPIHadoopDataset(job.getConfiguration())
    
    println("Completed HBaseWriter for " + phoenixConnection);
  }

}