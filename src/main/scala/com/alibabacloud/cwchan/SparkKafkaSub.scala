package com.alibabacloud.cwchan

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.ReadConfig
import com.typesafe.config.ConfigFactory

object SparkKafkaSub {

  var bootstrapServers: String = null;
  var groupId: String = null;
  var topicName: String = null;

  var kafkaLoginModule: String = null;
  var kafkaUsername: String = null;
  var kafkaPassword: String = null;
  var jassConfigEntity: String = null;
  val config = ConfigFactory.load().getConfig("com.alibaba-inc.cwchan");

  def loadConfig(): SparkSession = {
    val kafkaConfig = config.getConfig("Kafka");
    bootstrapServers = kafkaConfig.getString("bootstrapServers");
    groupId = kafkaConfig.getString("groupId");
    topicName = kafkaConfig.getString("topicName");

    println("running kafka subscriber for " + topicName);

    return SparkApp.sparkSession;
  }

  def loadMongo(): ReadConfig = {

    val mongoconfig = config.getConfig("MongoDB");
    val uri = mongoconfig.getString("uri");
    val db = mongoconfig.getString("db");
    val collection = mongoconfig.getString("collection");

    val readConfig = ReadConfig(
      Map(
        "uri" -> uri,
        "database" -> db,
        "collection" -> collection))

    return readConfig;
  }

  def run(): Unit = {

    val sparkSession = loadConfig();

    val dfStatic = MongoSpark.load(sparkSession, loadMongo())
      .selectExpr("symbol as Symbol", "CompanyName", "LastPrice", "Volume")

    val dfStream = sparkSession
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("subscribe", topicName)
      .option("group.id", groupId)
      .load()
      .selectExpr("CAST(key AS STRING) as SourceIP", "split(value, ',')[0] as StockCode", "split(value, ',')[1] as TS")

    val dfJoin = dfStream
      .join(dfStatic, dfStream("StockCode") === dfStatic("Symbol"), "leftouter")
      .selectExpr("SourceIP as ROWKEY", "StockCode as stock_code", "CompanyName as stock_name", "LastPrice as last_price", "TS as timestamp")

    SparkHBaseWriter.open();
    
    val query = dfJoin
      .writeStream
      .foreachBatch { (output: DataFrame, batchId: Long) =>
        SparkHBaseWriter.process(output);
      }
      .start
      .awaitTermination();

    SparkHBaseWriter.close();

    //    val query = dfJoin
    //      .writeStream
    //      .foreachBatch { (output: Dataset[Row], batchId: Long) =>
    //        SparkHBaseWriter.open();
    //        for (r <- output.collect()) {
    //          SparkHBaseWriter.process(r);
    //        }
    //        SparkHBaseWriter.close();
    //      }
    //      .start
    //      .awaitTermination();

  }

}