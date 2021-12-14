import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{IntegerSerializer, StringSerializer}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


object Main {

  def main(args: Array[String]): Unit = {

    val topicName = "hadoop_elephants"

    val producerProperties = new Properties()
    producerProperties.setProperty(
      ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"
    )
    producerProperties.setProperty(
      ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[IntegerSerializer].getName
    )
    producerProperties.setProperty(
      ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName
    )

    val producer = new KafkaProducer[Int, String](producerProperties)



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
    val website_map = website.map(x => x.split(",")).map(x => x(0))

    val _country_city = spark.sparkContext.textFile("input/CountryCities.csv")
      .map(x => x.split(",")).map(x => (x(0), x(1)))

    val products = spark.sparkContext.textFile("input/Products.csv")
    val productMap = products.map(x => x.split(",")).map(x => (x(0), x(1), x(2), x(3)))

    val names = spark.sparkContext.textFile("input/namesFile.csv")
    val namesMap = names.map(x => x.split(",")).map(x => (x(0), x(1)))

    def produceData2Stream(spark: SparkSession, namesMap:RDD[(String,String)], productMap:RDD[(String,String,String,String)], _country_city:RDD[(String,String)], website_map:RDD[String] ):String={
      val output = List(
        NameGenerator.generate(spark,namesMap),
        Product.generate(spark,productMap),
        AddressGenerator.generate(spark,_country_city),
        paymentType.generate,
        websiteGen.generate(spark,website_map),
        DateTimeGenerator.generate,
        quantityTXN.generate,
        paymentTXN.generate)
      output.mkString(",")
    }


    var i = 0
    /*
    val outFile = new CSV
    outFile.writeHeader()
    while (i < 20) {
      i+=1
      var output = ""
      output += Generator.generate()
      // Thread.sleep(2000)//2 seconds
      println(output)
      //producer.send(new ProducerRecord[Int, String](topicName, i, output))
      outFile.append(output)

    }*/
    AnalyticsEngine.Loop(AnalyticsEngine.Load(spark))
    //producer.flush()

  }
}
