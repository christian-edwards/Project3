
import org.apache.spark.sql.SparkSession

object Main {

  def main(args: Array[String]): Unit = {
    //System.setProperty("hadoop.home.dir", "C:\\hadoop")
    org.apache.log4j.Logger.getLogger("akka").setLevel(org.apache.log4j.Level.OFF)
    org.apache.log4j.Logger.getLogger("hive").setLevel(org.apache.log4j.Level.OFF)
    org.apache.log4j.Logger.getLogger("org").setLevel(org.apache.log4j.Level.OFF)

    val spark = SparkSession
      .builder
      .appName("Project3")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()
    //println("created spark session")

    val website = spark.sparkContext.textFile("input/websites.csv")
    val website_map = website.map(x=>x.split(",")).map(x=>x(0))

    val _country_city = spark.sparkContext.textFile("input/CountryCities.csv")
      .map(x => x.split(",")).map(x => (x(0), x(1)))

    var i = 0
    for(i <-0 to 10){
      var output = ""
      output += AddressGenerator.generate(spark,_country_city) + ","
      output += paymentType.generate + ","
      output += websiteGen.generate(spark,website_map) + ","
      output += DateTimeGenerator.generate + ","
      output += quantityTXN.generate + ","
      output += paymentTXN.generate
      Thread.sleep(100)
      println(output)
    }
  }
}
