import java.util.UUID
import scala.collection.mutable.Queue

object paymentTXN {
  val failureReasons = Array("Insufficient funds","Incorrect information","Transaction cancelled")
  val uniqueId = new Queue[UUID]
  val OrderId = new Queue[UUID]

  def generate():String = {
    val r = new scala.util.Random(System.currentTimeMillis)
    val x:Int = r.nextInt(100)

    val orderID = generateOrderID()
    var txID = ""
    if(x > 20){
      txID = generateTxID()
      return "Y," + txID
    } else {
      return "N"+","+failureReasons(r.nextInt(3))
    }
  }
  def generateTxID(): String = {
    uniqueId.enqueue(UUID.randomUUID())
    return uniqueId.dequeue().toString
  }

  def generateOrderID(): String ={

    OrderId.enqueue(UUID.randomUUID())
    return OrderId.dequeue().toString
  }
}
