

import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

import com.typesafe.config.ConfigFactory

object SparkReadOss {

  val config = ConfigFactory.load("application.conf").getConfig("com.alibaba-inc.cwchan")
  val ossConfig = config.getConfig("OSS")
  val accessKeyId: String = ossConfig.getString("accessKeyId")
  val accessKeySecret: String = ossConfig.getString("accessKeySecret")

  val endpoint: String = ossConfig.getString("endpoint")
  val bucketName: String = ossConfig.getString("bucketName")
  val inputobjectName: String = "/DLA/Input/test.csv"
  val outputobjectName: String = "/DLA/Output/test-out"
  
  var sparkSession: SparkSession = null;
  
  {
    
     val sparkConf = new SparkConf().setAppName("spark oss test")
      .set("spark.hadoop.fs.oss.impl", "org.apache.hadoop.fs.aliyun.oss.AliyunOSSFileSystem")
      .set("spark.hadoop.fs.oss.endpoint", "oss-cn-hongkong.aliyuncs.com")
      .set("spark.hadoop.fs.oss.accessKeyId", accessKeyId)
      .set("spark.hadoop.fs.oss.accessKeySecret", accessKeySecret)
    
    sparkSession = SparkSession
      .builder()
      .config(sparkConf)
      .master("local[4]")
      .getOrCreate()
  }
  
  def DLAReadOSS(): DataFrame =  {
   
    //oss path format: oss://your_bucket_name/your/path

    val ossData = sparkSession.read.format("csv").option("header", "true").load("oss://" + bucketName + inputobjectName)
    
    //ossData.write.format("csv").mode(SaveMode.Overwrite).save("oss://" + bucketName + outputobjectName)
    //ossData.write.csv("oss://" + bucketName + outputobjectName)
    
    
    return ossData;
  }
  
  def StopSpark(): Unit = {
    sparkSession.stop()
  }
}