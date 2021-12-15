import scala.io.StdIn.readInt
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions.{lower, upper}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.format_number

object AnalyticsEngine {

  def Load(spark: SparkSession): sql.DataFrame ={
    //val df = spark.read.option("header","true").option("inferSchema", "true").csv("Stream.csv")
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
      .add("ecommerce_website_name",StringType,true)
      .add("payment_txn_id",StringType,true)
      .add("payment_txn_success",StringType,true)
      .add("failure_reason",StringType,true)
    val df = spark.read.schema(schema).csv("transactions.csv")
    df
  }

  def Loop(df: sql.DataFrame): Unit ={
    var exit = true
    df.createOrReplaceGlobalTempView("InputData")
    while(exit){
      print("Query: select ")
      val inLine = readLine()

      inLine.split(" ")(0).toUpperCase() match{
        case "QUERY1" =>
          println("Query 1: What is the total amount of money spent per each payment type?")
          df.sparkSession.sql(s"SELECT t1.payment_type, Concat('$$',round(sum(total_price),2)) AS payment_totals_in_billions FROM (SELECT payment_type,qty * substring(price,2) as total_price FROM global_temp.InputData WHERE payment_txn_success == 'Y') t1 GROUP BY payment_type ORDER BY payment_totals_in_billions desc").show()
        case "QUERY2" =>
          println("Query 2: What was the transactional success/failure rate and what were the leading reasons for a transaction failure?")
          df.sparkSession.sql(s"with "+
            "cte1 as (select payment_txn_success, format_number(count(*)/(select count(*) from global_temp.InputData)*100, 2) as Percent from global_temp.InputData group by payment_txn_success order by Percent desc), "+
            "cte2 as (select failure_reason, format_number(count(*)/(select count(*) from global_temp.InputData where payment_txn_success == 'N')*100, 2) as Percent from global_temp.InputData where payment_txn_success == 'N' group by failure_reason order by Percent desc) "+
            "select * from cte1 union select * from cte2 ").show(false)
        case "QUERY3" =>
          println("Query 3: What were the top 10 Highest selling items based on quantity?")
          df.sparkSession.sql(s"Select product_name,sum(qty) total_qty FROM global_temp.InputData GROUP BY product_name ORDER BY total_qty desc").show(10)
        case "QUERY4" =>
          // Top 10 highest earning days
          println("Query 4: What were the Top 10 highest earning days?")
          df.where("payment_txn_success = 'Y'")
            .withColumn("date_format", org.apache.spark.sql.functions.substring(org.apache.spark.sql.functions.col("datetime"), 0, 10))
            .withColumn("price_format", org.apache.spark.sql.functions.regexp_replace(org.apache.spark.sql.functions.col("price"), "[$, ]", ""))
            .withColumn("earnings", org.apache.spark.sql.functions.col("qty") * org.apache.spark.sql.functions.col("price_format"))
            .select("date_format", "earnings")
            .groupBy("date_format")
            .agg(org.apache.spark.sql.functions.sum("earnings"))
            .withColumn("earnings_format", org.apache.spark.sql.functions.format_number(org.apache.spark.sql.functions.col("sum(earnings)"), 2))
            .withColumnRenamed("date_format", "Date")
            .withColumnRenamed("earnings_format", "Daily_Earnings_($)")
            .select("Date", "Daily_Earnings_($)")
            .limit(10)
            .show()
        case "QUERY5" =>
          println("Query 5: Which city had the most total traffic?")
             df.sparkSession.sql(s"SELECT tbl3. city AS City, COUNT(tbl3.order_id) AS Traffic FROM (SELECT * FROM global_temp.InputData WHERE ecommerce_website_name IN (SELECT ecommerce_website_name FROM (SELECT ecommerce_website_name, count(*) AS traffic FROM global_temp.InputData GROUP BY ecommerce_website_name) AS tbl1 ORDER BY tbl1.traffic DESC LIMIT 1)) AS tbl3 GROUP BY tbl3.city ORDER BY Traffic DESC").show()
        case "QUERY6" =>
          println("Query 6: Which category of products sold the worst?")
          df.sparkSession.sql(s"select product_category, count(*) as Sales from global_temp.InputData where payment_txn_success == 'Y' group by product_category order by Sales asc").show(false)
        case "EXIT" =>
          return
        case default =>
          try {
            //df.sparkSession.sql("select * from global_temp.InputData").show()
            df.sparkSession.sql(s"""select $inLine""").show(2000)
          } catch {
            case x: org.apache.spark.sql.AnalysisException => println("Please enter valid input or \"EXIT\" to exit.") // in case query is wrong
          }
      }

    }
  }

  def main(args: Array[String]): Unit = {
    org.apache.log4j.Logger.getLogger("akka").setLevel(org.apache.log4j.Level.OFF)
    org.apache.log4j.Logger.getLogger("hive").setLevel(org.apache.log4j.Level.OFF)
    org.apache.log4j.Logger.getLogger("org").setLevel(org.apache.log4j.Level.OFF)

    val spark = SparkSession
      .builder
      .appName("Project3_AnalyticsEngine")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()

    Loop(Load(spark))
    spark.stop()
  }
}
