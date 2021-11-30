object Main {

  def main(args: Array[String]): Unit = {
    var i = 0
    for(i <-0 to 10){
      val address = AddressGenerator.generate()
      val test = paymentTXN.generate()
      println(s"$address,$test")
      Thread.sleep(2000)
    }
  }
}
