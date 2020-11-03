package com.alibabacloud.cwchan

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.SparkSession

import com.typesafe.config.ConfigFactory

object SparkHBaseWriter extends App {

  var sparkSessoin: SparkSession = null;
  var phoenixConnection: String = null;
  var zookeeperQuorum: String = null;

  def loadConfig(): SparkSession = {

    val config = ConfigFactory.load().getConfig("com.alibaba-inc.cwchan");
    val phoenixConfig = config.getConfig("Phoenix");
    val port = phoenixConfig.getString("port");
    zookeeperQuorum = phoenixConfig.getString("zookeeper");
    return SparkApp.sparkSessoin
  }

  def run(): Unit = {

    var sparkSession = loadConfig();

    

    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum", zookeeperQuorum)
    hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, "USER_TEST:EMP")
    val conn = ConnectionFactory.createConnection(hbaseConf)
    val admin = conn.getAdmin

    println("running hbase writer for " + hbaseConf.get(TableOutputFormat.OUTPUT_TABLE));
    
    val tableDescr = new HTableDescriptor(TableName.valueOf(hbaseConf.get(TableOutputFormat.OUTPUT_TABLE)))
    tableDescr.addFamily(new HColumnDescriptor("personal".getBytes))

    //    if (admin.tableExists(TableName.valueOf(hbaseConf.get(TableOutputFormat.OUTPUT_TABLE)))) {
    //      admin.disableTable(TableName.valueOf(hbaseConf.get(TableOutputFormat.OUTPUT_TABLE)));
    //      admin.deleteTable(TableName.valueOf(hbaseConf.get(TableOutputFormat.OUTPUT_TABLE)));
    //    }

    if (!admin.tableExists(TableName.valueOf(hbaseConf.get(TableOutputFormat.OUTPUT_TABLE)))) {
      admin.createTable(tableDescr)
    }

    val job = new Job(hbaseConf)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])

    val dataArray = ArrayBuffer[String]();
    for (a <- 1 to 10) {
      dataArray += a + ",chiwai.chan," + LocalDateTime.now.format(DateTimeFormatter.ofPattern("YYYYMMdd_HHmmss"))
    }

    val indataRDD = sparkSession.sparkContext.makeRDD(dataArray)

    val rdd = indataRDD.map(_.split(',')).map { arr =>
      val put = new Put(Bytes.toBytes(arr(0)));
      put.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("name"), Bytes.toBytes(arr(1)))
      put.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("registerts"), Bytes.toBytes(arr(2)))
      (new ImmutableBytesWritable, put)
    }
    rdd.saveAsNewAPIHadoopDataset(job.getConfiguration())

    sparkSession.close();
  }

}