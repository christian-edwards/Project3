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
    val df = spark.read.schema(schema).csv("transactions.csv").cache()
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
          //TODO: Query 1
          df.sparkSession.sql(s"SELECT t1.payment_type, round(sum(total_price),2) AS payment_totals FROM (SELECT payment_type,qty * substring(price,2) as total_price FROM global_temp.InputData WHERE payment_txn_success == 'Y') t1 GROUP BY payment_type ORDER BY payment_totals desc").show()
        case "QUERY2" =>
          df.sparkSession.sql(s"with "+
            "cte1 as (select payment_txn_success, format_number(count(*)/(select count(*) from global_temp.InputData)*100, 2) as Percent from global_temp.InputData group by payment_txn_success order by Percent desc), "+
            "cte2 as (select failure_reason, format_number(count(*)/(select count(*) from global_temp.InputData where payment_txn_success == 'N')*100, 2) as Percent from global_temp.InputData where payment_txn_success == 'N' group by failure_reason order by Percent desc) "+
            "select * from cte1 union select * from cte2 ").show(false)
        case "QUERY3" =>
          //TODO: Query 3
          df.sparkSession.sql(s"Select product_name,sum(qty) total_qty FROM global_temp.InputData GROUP BY product_name ORDER BY total_qty desc").show(10)
        case "QUERY4" =>
          // Top 10 highest earning days
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
          //TODO: Query 5
        case "QUERY6" =>
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

      /*
      if(inLine.split(" ")(0).toUpperCase() == "EXIT"){
        exit = false
      } else if(inLine.split(" ")(0).toUpperCase() == "QUERY2"){
        df.sparkSession.sql(s"with "+
          "cte1 as (select payment_txn_success, format_number(count(*)/(select count(*) from global_temp.InputData)*100, 2) as Percent from global_temp.InputData group by payment_txn_success order by Percent desc), "+
          "cte2 as (select failure_reason, format_number(count(*)/(select count(*) from global_temp.InputData where payment_txn_success == 'N')*100, 2) as Percent from global_temp.InputData where payment_txn_success == 'N' group by failure_reason order by Percent desc) "+
          "select * from cte1 union select * from cte2 ").show(false)
      } else if(inLine.split(" ")(0).toUpperCase() == "QUERY6"){
        df.sparkSession.sql(s"select product_category, count(*) as Sales from global_temp.InputData where payment_txn_success == 'Y' group by product_category order by Sales asc").show(false)
      } else try {
        //df.sparkSession.sql("select * from global_temp.InputData").show()
        df.sparkSession.sql(s"""select $inLine""").show(2000)
      } catch {
        case x: org.apache.spark.sql.AnalysisException => println("incorrect query") // in case query is wrong
      }
      */
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
