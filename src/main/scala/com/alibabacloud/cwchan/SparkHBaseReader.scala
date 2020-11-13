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
    val port = phoenixConfig.getString("port");
    zookeeperQuorum = phoenixConfig.getString("zookeeper");

    return SparkApp.sparkSession
  }

  def run(): Unit = {

    var sparkSession = loadConfig();
    
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum", zookeeperQuorum)
    hbaseConf.set(TableInputFormat.INPUT_TABLE, "USER_TEST:EMP")

    println("running hbase reader for " + hbaseConf.get(TableInputFormat.INPUT_TABLE));
    
    val hBaseRDD = sparkSession.sparkContext.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])

    hBaseRDD.foreach{ case (_ ,result) =>
      val key = Bytes.toString(result.getRow)
      val name = Bytes.toString(result.getValue("personal".getBytes,"name".getBytes))
      val age = Bytes.toString(result.getValue("personal".getBytes,"registerts".getBytes))
      println("Rowkey:"+key+"\tpersonal.name:"+name+"\tpersonal.registerts:"+age)
    }
  }

}