object AddressGenerator {
  // Suppress messages to console
  org.apache.log4j.Logger.getLogger("akka").setLevel(org.apache.log4j.Level.OFF)
  org.apache.log4j.Logger.getLogger("org").setLevel(org.apache.log4j.Level.OFF)

  private val _spark = org.apache.spark.sql.SparkSession
    .builder()
    .appName("Project 3")
    .master("local")
    .getOrCreate()
  private val _country_city = _spark.sparkContext.textFile("input/CountryCities.csv")
    .map(x => x.split(",")).map(x => (x(0), x(1)))

  def generate(): String = {
    val sample = _country_city.takeSample(withReplacement = true, 1)(0)
    s"${sample._1},${sample._2}"
  }
}