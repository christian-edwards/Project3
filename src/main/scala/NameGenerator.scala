import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import java.util

object NameGenerator {

  def generate(spark:SparkSession,namesMap:RDD[(String,String)]): String = {
    val randomName = namesMap.takeSample(withReplacement = true, 1)(0)
    s"${randomName._1},${randomName._2}"

  }
}