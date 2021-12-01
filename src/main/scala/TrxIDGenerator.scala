import scala.util.Random
import java.sql.Timestamp
import java.time.{LocalDateTime, ZoneOffset}
import java.time.format.DateTimeFormatter
import org.apache.spark.sql.functions._
import org.apache.spark.util._
import java.util.Calendar
import java.util.concurrent.BlockingQueue
import java.util.concurrent.LinkedBlockingQueue
import java.util.UUID
//import scala.actors._
//import scala.actors.Actor._
import scala.collection.mutable.Queue

import java.util.UUID

object TrxIDGenerator extends App {
 // val curTime = Calendar.getInstance().getTime()
 // val TxID = Seq(Timestamp.valueOf(LocalDateTime.of(Lo))
 // println(TxID);

  def generateTxID(): String = {
    val uniqId = new Queue[UUID]
    uniqId.enqueue(UUID.randomUUID())
    return uniqId.dequeue().toString
  }

  def generateOrderID(): String ={
    val OrderId = new Queue[UUID]
    OrderId.enqueue(UUID.randomUUID())
    return OrderId.dequeue().toString
  }

  do{
    println("Order ID: " + generateOrderID )
    println("Transaction ID : " + generateTxID )

  }while (true)
}

