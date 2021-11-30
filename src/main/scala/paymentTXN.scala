class paymentTXN(){

  def generate():String = {
    val failureReasons = Array("Insufficient funds","Incorrect information","Transaction canceled")

    val r = new scala.util.Random(System.currentTimeMillis)
    val x:Int = r.nextInt(100)
    if(x > 50){
      return "Y"
    } else {
      return "N"+","+failureReasons(r.nextInt(3))
    }
  }


}
