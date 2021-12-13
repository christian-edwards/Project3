import scala.io.StdIn.readInt
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions.{lower, upper}

object AnalyticsEngine {

  def Load(spark: SparkSession): sql.DataFrame ={
    val df = spark.read.option("header","true").option("inferSchema", "true").csv("Stream.csv")
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
      } else try {
        //df.sparkSession.sql("select * from global_temp.InputData").show()
        df.sparkSession.sql(s"select $inLine").show(2000)
      } catch {
        case x: org.apache.spark.sql.AnalysisException => println("incorrect query") // in case query is wrong
      }
    }
  }
}
