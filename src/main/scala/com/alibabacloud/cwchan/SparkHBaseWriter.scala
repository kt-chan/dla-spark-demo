package com.alibabacloud.cwchan

import java.io.IOException

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.NamespaceDescriptor
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Table
import org.apache.hadoop.hbase.client.TableDescriptorBuilder
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.ForeachWriter
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession

import com.typesafe.config.ConfigFactory

class SparkHBaseWriter extends ForeachWriter[Row] {

  var sparkSessoin: SparkSession = null
  var hbaseTable: Table = null;
  var hbaseConn: Connection = null;
  var phoenixConnection: String = null;
  var zookeeperQuorum: String = null;

  def loadConfig(): SparkSession = {

    val config = ConfigFactory.load().getConfig("com.alibaba-inc.cwchan");
    val phoenixConfig = config.getConfig("Phoenix");
    val port = phoenixConfig.getString("port");
    zookeeperQuorum = phoenixConfig.getString("zookeeper");
    return SparkApp.sparkSessoin;
  }

  def setupHBaseJob(): Table = {
    val hbaseConf = HBaseConfiguration.create();
    hbaseConf.set("hbase.zookeeper.quorum", zookeeperQuorum);
    hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, "USER_TEST:EMP");
    hbaseConn = ConnectionFactory.createConnection(hbaseConf);
    val admin = hbaseConn.getAdmin();

    println("running hbase writer for " + hbaseConf.get(TableOutputFormat.OUTPUT_TABLE));

    var listNamespaceDescriptor: NamespaceDescriptor = null;
    try {
      listNamespaceDescriptor = admin.getNamespaceDescriptor("USER_TEST")
    } catch {
      case e: IOException =>
        {
          println("Namespace: \"USER_TEST\" doesnot exist, creating one ...")
          admin.createNamespace(NamespaceDescriptor.create("USER_TEST").build());
        };
    }

    val tableDescr = TableDescriptorBuilder.newBuilder(TableName.valueOf(hbaseConf.get(TableOutputFormat.OUTPUT_TABLE)))
    tableDescr.setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder("personal".getBytes).build());

    if (!admin.tableExists(TableName.valueOf(hbaseConf.get(TableOutputFormat.OUTPUT_TABLE)))) {
      admin.createTable(tableDescr.build());
    }

    return hbaseConn.getTable(TableName.valueOf(hbaseConf.get(TableOutputFormat.OUTPUT_TABLE)));
  }

  override def open(partitionId: Long, version: Long): Boolean = {
    this.sparkSessoin = loadConfig();
    this.hbaseTable = setupHBaseJob();
    return true;

  }

  override def close(errorOrNull: Throwable): Unit = {
    try {
      hbaseTable.close();
      hbaseConn.close();
    } catch {
      case e: IOException => println(e.printStackTrace())
    }
  }

  override def process(record: Row): Unit = {

    println("input data in row is: " + record.toString());
    val rowkey = record.getString(0).split(",");
    val rowval = record.getString(1).split(",");

    val put = new Put(Bytes.toBytes(rowkey(0)));
    put.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("name"), Bytes.toBytes(rowval(0)));
    put.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("registerts"), Bytes.toBytes(rowval(1)));
    this.hbaseTable.put(put);
  }
}