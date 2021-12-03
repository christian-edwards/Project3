import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession


object websiteGen{

  def generate(spark:SparkSession,website_map:RDD[String]): String ={
    val web = website_map.takeSample(true,1)
    return web(0)
  }
}
