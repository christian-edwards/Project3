import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

object AnalyticsEngine {

  def main(args:Array[String]) : Unit = {
    connect()
    showData()
  }

  private var spark:SparkSession = _
  def connect() : Unit = {
    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    spark = SparkSession
      .builder()
      .appName("CONSUMER DATA")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    spark.sql("DROP table IF EXISTS consumer_data")
//    spark.sql("CREATE table IF NOT exists consumer_data(order_id String, customer_id String, customer_name String, product_id String, product_name String, product_category String," +
//      " payment_type String, qty Int, price String, dateTime Date, country String, city String, website String, payment_txn_id String, payment_txn_success String)" +
//      "ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'")
//
//    spark.sql("Load data Local Inpath 'Stream.csv' into table consumer_data")
  }

  def showData(): Unit ={
    val schema = new StructType()
      .add("order_id",IntegerType,true)
      .add("customer_id",IntegerType,true)
      .add("customer_name",StringType,true)
      .add("product_id",IntegerType,true)
      .add("product_name",StringType,true)
      .add("product_category",StringType,true)
      .add("payment_type",StringType,true)
      .add("qty",IntegerType,true)
      .add("price",StringType,true)
      .add("datetime",StringType,true)
      .add("country",StringType,true)
      .add("city",StringType,true)
      .add("website",StringType,true)
      .add("payment_txn_id",StringType,true)
      .add("payment_txn_success",StringType,true)

    val consumer_data = spark.read.option("delimiter",",").schema(schema).csv("transactions.csv")
    consumer_data.createOrReplaceTempView("consumer_data")

    val sqlDF = spark.sql("Select * FROM consumer_data").show()

//    val paymentTypeAmt = spark.sql("WITH " +
//  "total_price AS (Select payment_type, substring(price,2) * qty as total_price FROM consumer_data) " +
//  "SELECT * FROM total_price").show()

    //Top 10 Highest selling items based on quantity
    val top10Qty = spark.sql("Select product_name,sum(qty) total_qty FROM consumer_data GROUP BY product_name ORDER BY total_qty desc").show(10)

    // Total amount of money spent per each Payment Type
//    val totPerPaymentType = spark.sql("SELECT order_id,payment_type,round(qty * substring(price,2),2) as total_price FROM consumer_data").show()

    val totPerPaymentType2 = spark.sql("SELECT t1.payment_type, round(sum(total_price),2) AS payment_totals FROM (SELECT payment_type,qty * substring(price,2) as total_price FROM consumer_data WHERE payment_txn_success = 'Y') t1 GROUP BY payment_type ORDER BY payment_totals desc").show()

  }

}
