import java.util.UUID
import scala.collection.mutable.Queue

object paymentTXN {
  val failureReasons = Array("Insufficient funds","Incorrect information","Transaction cancelled")

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
    val uniqId = new Queue[UUID]
    uniqId.enqueue(UUID.randomUUID())
    return uniqId.dequeue().toString
  }

  def generateOrderID(): String ={
    val OrderId = new Queue[UUID]
    OrderId.enqueue(UUID.randomUUID())
    return OrderId.dequeue().toString
  }
}
