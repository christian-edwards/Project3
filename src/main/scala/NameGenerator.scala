import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import java.util

object NameGenerator {
  /*
  var outList = new util.ArrayList[String]();

  val bufferedSource = io.Source.fromFile("input/namesFile.csv")
  for (line <- bufferedSource.getLines) {
    val cols = line.split(",").map(_.trim)
    val a= s"${cols(0)},${cols(1)}"
    outList.add(a)
  }
  bufferedSource.close
*/
  def generate(spark:SparkSession,namesMap:RDD[(String,String)]): String = {
    val randomName = namesMap.takeSample(withReplacement = true, 1)(0)
    s"${randomName._1},${randomName._2}"

  }
}