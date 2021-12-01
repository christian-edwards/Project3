object Generator {
  // Suppress messages to console
  org.apache.log4j.Logger.getLogger("akka").setLevel(org.apache.log4j.Level.OFF)
  org.apache.log4j.Logger.getLogger("hive").setLevel(org.apache.log4j.Level.OFF)
  org.apache.log4j.Logger.getLogger("org").setLevel(org.apache.log4j.Level.OFF)

  private val _spark = org.apache.spark.sql.SparkSession
    .builder
    .appName("Project3")
    .config("spark.master", "local")
    .enableHiveSupport()
    .getOrCreate()

  // Load input files
  private val _country_city = _spark.sparkContext.textFile("input/CountryCities.csv")
    .map(x => x.split(",")).map(x => (x(0), x(1)))

  def generate(): String = {
    DateTimeGenerator.generate()
    AddressGenerator.generate()
  }

  private object AddressGenerator {
    def generate(): String = {
      val sample = _country_city.takeSample(withReplacement = true, 1)(0)
      s"${sample._1},${sample._2}"
    }
  }

  private object DateTimeGenerator {
    def generate(): String = {
      val r1 = new scala.util.Random()
      val days = r1.nextInt(366)
      val minutes = r1.nextInt(1440)
      val dateStart = java.time.LocalDate.of(2020, 11, 30)
      val timeStart = java.time.LocalTime.of(0, 0, 0)
      val Date = dateStart.plusDays(days)
      val Time = timeStart.plusMinutes(minutes)
      s"$Date $Time"
    }
  }
}
