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

  // Call generate() from elsewhere
  def generate(): String = {
    s"${ProductGenerator.generate()}," +
      s"${PaymentTypeGenerator.generate()}," +
      s"${QuantityTransactionGenerator.generate()}," +
      s"${DateTimeGenerator.generate()}," +
      s"${AddressGenerator.generate()}," +
      s"${PaymentTransactionGenerator.generate()}"
  }

  private object AddressGenerator {
    private val _country_city = _spark.sparkContext.textFile("input/CountryCities.csv")
      .map(x => x.split(",")).map(x => (x(0), x(1)))

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

  private object PaymentTransactionGenerator {
    val failureReasons: Array[String] = Array("Insufficient funds","Incorrect information","Transaction cancelled")

    def generate(): String = {
      val r = new scala.util.Random(System.currentTimeMillis)
      val x: Int = r.nextInt(100)
      if(x > 20) {
        "Y"
      } else {
        s"N,${failureReasons(r.nextInt(3))}"
      }
    }
  }

  private object PaymentTypeGenerator {
    val payments: Array[String] = Array("Card", "Internet Banking", "UPI", "Wallet")

    def generate(): String = {
      val r = new scala.util.Random(System.currentTimeMillis())
      val x = r.nextInt(100)
      if (x > 20){
        payments(r.nextInt(2))
      }
      else{
        payments(r.nextInt(4))
      }
    }
  }

  private object ProductGenerator {
    private val _products = _spark.sparkContext.textFile("input/Products.csv")
      .map(x=>x.split(",")).map(x=>(x(0),x(1),x(2),x(3)))

    def generate(): String = {
      val randomProduct = _products.takeSample(withReplacement = true,1)
      s"${randomProduct(0)._1},${randomProduct(0)._4},${randomProduct(0)._3},${randomProduct(0)._2}"
    }
  }

  private object QuantityTransactionGenerator {
    var theCount = 0  // counts # of times called, hopefully // used to generate trends ie. after a while, our business will grow
    var theCountess :Array[Int] = new Array(100)
    val theWarehouse: Array[String] = Array("Camera Accessories", "Car Electronics & GPS", "Cell Phones & Accessories", "Computer & Accessories", "Computers & Accessories", "Digital Cameras", "Hard Drives", "Headphones", "Home Audio", "Home Security", "Media Players & Recorders", "Memory Cards", "Mirrorless System Lenses", "Portable Bluetooth Speakers", "Power Supplies", "Projection Screens & Material", "Security Cameras", "Smart Watches", "Solid State Drives", "Speaker Cables", "Speaker Systems", "Streaming Media Players", "TV", "TV Accessories & Parts", "Video Games", "Wireless Speakers")
    val rando = new scala.util.Random(System.currentTimeMillis)
    var i = 0

    // repeated calls without any parameters will increase the number returned by this function
    def generate(): String = { //generate with basic trending
      this.theCount += 1
      i = 0
      if (this.theCount > 25) { // end of our trend
        this.i  = this.rando.nextInt(250) + 250 // upToRoof + floor
      } else if (this.theCount > 10) { // small number
        this.i  = this.rando.nextInt(150) + 100
      } else { // smallest number
        this.i = this.rando.nextInt(100)
      }
      i.toString
    }

    // parameter of type boolean will just ignore the counters
    //    works for true or false
    def generate(killTrend: Boolean): String = {
      i = 0
      this.i = this.rando.nextInt(1000)
      i.toString
    }

    //  separate counters for each category type --declared up top--
    def generate(category: String): String = {
      val ID = this.theWarehouse.indexOf(category)
      if (ID < 0) {
        return "WTF was that again?"
      }
      this.theCountess(ID) += 1 // instead of generic count
      i = 0
      i = this.rando.nextInt(100)
      if (this.theCountess(ID) >= 10) { // insert various conditions here
        i += 100
      }
      i.toString
    }
  }
}
