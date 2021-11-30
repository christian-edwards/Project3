object Main {

  def main(args: Array[String]): Unit = {
    var i = 0
    for(i <-0 to 10){
      println(quantityTXN.generate("VCR"))
      Thread.sleep(500)
      println(quantityTXN.generate("VCR"))
      Thread.sleep(500)
      println(quantityTXN.generate("SSD"))
      Thread.sleep(500)
    }
  }
}
