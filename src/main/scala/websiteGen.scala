import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
object websiteGen {
  org.apache.log4j.Logger.getLogger("akka").setLevel(org.apache.log4j.Level.OFF)
  org.apache.log4j.Logger.getLogger("hive").setLevel(org.apache.log4j.Level.OFF)
  org.apache.log4j.Logger.getLogger("org").setLevel(org.apache.log4j.Level.OFF)
  val spark = SparkSession
    .builder
    .appName("hello hive")
    .config("spark.master", "local")
    .enableHiveSupport()
    .getOrCreate()
  println("created spark session")



  val website = spark.sparkContext.textFile("input/websites.csv")
  val website_map = website.map(x=>x.split(",")).map(x=>x(0))
  def generate(): String ={
    val web = website_map.takeSample(true,1)
    java.lang.Thread.sleep(2000)
    return web(0)
  }
}
