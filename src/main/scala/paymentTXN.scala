object paymentTXN {

  def generate():String = {
    val failureReasons = Array("Insufficient funds","Incorrect information","Transaction cancelled")

    val r = new scala.util.Random(System.currentTimeMillis)

    val x:Int = r.nextInt(100)
    if(x > 20){
      return "Y"
    } else {
      return "N"+","+failureReasons(r.nextInt(3))
    }
  }

}
