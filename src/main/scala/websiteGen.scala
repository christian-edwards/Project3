import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
object websiteGen {
  System.setProperty("hadoop.home.dir", "C:\\Program Files (x86)\\hadoop")
  val spark = SparkSession
    .builder
    .appName("hello hive")
    .config("spark.master", "local")
    .enableHiveSupport()
    .getOrCreate()
  println("created spark session")
  spark.sparkContext.setLogLevel("ERROR")
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  Logger.getLogger("org.apache.spark").setLevel(Level.OFF)

  val website = spark.sparkContext.textFile("input/websites.csv")
  val website_map = website.map(x=>x.split(",")).map(x=>x(0))
  def generate(): String ={
    val web = website_map.takeSample(true,1)
    java.lang.Thread.sleep(2000)
    return web(0)
  }
}
