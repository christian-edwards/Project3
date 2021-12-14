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
    }
  }
}
