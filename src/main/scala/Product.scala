import org.apache.parquet.format.LogicalType.JSON
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD

object Product {
/*
  org.apache.log4j.Logger.getLogger("akka").setLevel(org.apache.log4j.Level.OFF)
  org.apache.log4j.Logger.getLogger("hive").setLevel(org.apache.log4j.Level.OFF)
  org.apache.log4j.Logger.getLogger("org").setLevel(org.apache.log4j.Level.OFF)
  val spark = org.apache.spark.sql.SparkSession
    .builder()
    .appName("Test")
    .config("spark.master", "local")
    .getOrCreate()
  val products = spark.sparkContext.textFile("input/Products.csv")
  val productMap = products.map(x=>x.split(",")).map(x=>(x(0),x(1),x(2),x(3)))
 */
  def generate(spark:SparkSession,productMap:RDD[(String,String,String,String)]) : String = {
    //val productMap = products.map(x=>x.split(",")).map(x=>(x(0),x(1),x(2),x(3)))
    val randomProduct = productMap.takeSample(true,1)
    val productString = randomProduct(0)._1 + ",$" + randomProduct(0)._2 + "," + randomProduct(0)._3 + "," + randomProduct(0)._4
    return productString
  }





}
