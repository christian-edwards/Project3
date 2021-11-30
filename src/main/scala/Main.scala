object Main {

  def main(args: Array[String]): Unit = {
    var i = 0
    for(i <-0 to 10){
      val test = paymentTXN.generate
      val webtest = websiteGen.generate
      val payment = paymentType.generate
      Thread.sleep(100)
      println(test, webtest, payment)
    }


  }
}
