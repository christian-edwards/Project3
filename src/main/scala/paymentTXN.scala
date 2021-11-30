class paymentTXN(){

  def generate():String = {
    val failureReasons = Array("Insufficient funds","Invalid information","Transaction canceled")

    val r = new scala.util.Random(System.currentTimeMillis)
    val x = r.nextInt()
    print(x)
    if(x > 50){
      return "Y"
    } else {
      return "N"+","+failureReasons(r.nextInt(3))
    }
  }


}
