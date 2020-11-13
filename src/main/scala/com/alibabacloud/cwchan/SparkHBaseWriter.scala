package com.alibabacloud.cwchan

import java.io.IOException
import java.sql.DriverManager
import java.sql.ResultSet
import java.sql.SQLException
import java.sql.Statement
import java.text.SimpleDateFormat
import java.util.Date
import java.util.Properties

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.NamespaceDescriptor
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.client.Table
import org.apache.hadoop.hbase.client.TableDescriptorBuilder
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession

import com.typesafe.config.ConfigFactory

object SparkHBaseWriter {

  var sparkSessoin: SparkSession = null
  var hbaseTable: Table = null;
  var hbaseConn: Connection = null;
  var phoenixConnection: String = null;
  var zookeeperQuorum: String = null;
  var dateFormater = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  var sqlStmt: java.sql.Statement = null

  def open(): Unit = {
    try {
      val driver = "org.apache.phoenix.queryserver.client.Driver"
      val queryServerAddress = ConfigFactory.load().getConfig("com.alibaba-inc.cwchan").getConfig("HBase").getString("phoenix")
      val url = "jdbc:phoenix:thin:url=" + queryServerAddress + ";serialization=PROTOBUF"
      Class.forName(driver).newInstance()
      sqlStmt = DriverManager.getConnection(url).createStatement
    } catch {
      case e: Throwable => println(e.printStackTrace())
    }
  }

  def close(): Unit = {
    try {
      sqlStmt.getConnection.close()
      sqlStmt.close();
    } catch {
      case e: Throwable => println(e.printStackTrace())
    }
  }

  def process(record: Row): Unit = {

    println("input data in row is: " + record.toString());

    val rowkey = record.getString(0);
    val stockCode = record.getString(1);
    val companyName = record.getString(2);
    val lastPrice = record.getDouble(3).toString();
    val ts = dateFormater.format(new Date(record.getString(4).toLong)).toString()

    val sql = "UPSERT INTO USER_TEST.STOCK (ROWKEY, \"org\".\"stock_code\", \"org\".\"stock_name\",  \"org\".\"last_price\", \"org\".\"timestamp\") VALUES (" +
      "\'" + rowkey + "\'," +
      "\'" + stockCode + "\'," +
      "\'" + companyName + "\'," +
      lastPrice + "," +
      "\'" + ts + "\'" +
      ")"

    if (sqlStmt.execute(sql)) {
      val rs: ResultSet = sqlStmt.getResultSet();
      val rsmd = rs.getMetaData();
      val columnCount = rsmd.getColumnCount();

      while (rs.next()) {

        for (i <- 1 to columnCount) {
          System.out.print(rs.getString(i + 1) + "/t");
        }
        System.out.print("/n");
      }
    } else {
      System.out.println("UPSERTED Records: " + sqlStmt.getUpdateCount() + ".");
    }
  }

  //  def loadConfig(): SparkSession = {
  //
  //    val config = ConfigFactory.load().getConfig("com.alibaba-inc.cwchan");
  //    zookeeperQuorum = config.getConfig("HBase").getString("zookeeper");
  //    return SparkApp.sparkSession;
  //  }
  //
  //  def setupHBaseJob(): Table = {
  //    val hbaseConf = HBaseConfiguration.create();
  //    hbaseConf.set("hbase.zookeeper.quorum", zookeeperQuorum);
  //    hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, "USER_TEST:STOCK");
  //    hbaseConn = ConnectionFactory.createConnection(hbaseConf);
  //    val admin = hbaseConn.getAdmin();
  //
  //    var listNamespaceDescriptor: NamespaceDescriptor = null;
  //    try {
  //      listNamespaceDescriptor = admin.getNamespaceDescriptor("USER_TEST")
  //    } catch {
  //      case e: IOException =>
  //        {
  //          println("Namespace: \"USER_TEST\" doesnot exist, creating one ...")
  //          admin.createNamespace(NamespaceDescriptor.create("USER_TEST").build());
  //        };
  //    }
  //
  //    val tableDescr = TableDescriptorBuilder.newBuilder(TableName.valueOf(hbaseConf.get(TableOutputFormat.OUTPUT_TABLE)))
  //    tableDescr.setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder("org".getBytes).build());
  //
  //    if (!admin.tableExists(TableName.valueOf(hbaseConf.get(TableOutputFormat.OUTPUT_TABLE)))) {
  //      admin.createTable(tableDescr.build());
  //    }
  //
  //    return hbaseConn.getTable(TableName.valueOf(hbaseConf.get(TableOutputFormat.OUTPUT_TABLE)));
  //  }
  //
  //  def open(): Boolean = {
  //    println("Open connection to HBase");
  //    try {
  //      this.sparkSessoin = loadConfig();
  //      this.hbaseTable = setupHBaseJob();
  //    } catch {
  //      case e: Throwable => println(e.printStackTrace())
  //    }
  //
  //    return true;
  //
  //  }
  //
  //  def close(): Unit = {
  //    println("Close connection to HBase");
  //    try {
  //      hbaseTable.close();
  //      hbaseConn.close();
  //    } catch {
  //      case e: Throwable => println(e.printStackTrace())
  //    }
  //  }
  //
  //  def process(record: Row): Unit = {
  //    println("input data in row is: " + record.toString());
  //
  //    val rowkey = record.getString(0);
  //    val stockCode = record.getString(1);
  //    val companyName = record.getString(2);
  //    val lastPrice = record.getDouble(3).toString();
  //    val ts = dateFormater.format(new Date(record.getString(4).toLong)).toString()
  //
  //    val put = new Put(Bytes.toBytes(rowkey));
  //    put.addColumn(Bytes.toBytes("org"), Bytes.toBytes("stock_code"), Bytes.toBytes(stockCode));
  //    put.addColumn(Bytes.toBytes("org"), Bytes.toBytes("stock_name"), Bytes.toBytes(companyName));
  //    put.addColumn(Bytes.toBytes("org"), Bytes.toBytes("last_price"), Bytes.toBytes(lastPrice));
  //    put.addColumn(Bytes.toBytes("org"), Bytes.toBytes("timestamp"), Bytes.toBytes(ts));
  //    this.hbaseTable.put(put);
  //  }
}