object Main {

  def main(args: Array[String]): Unit = {
    var i = 0
    for(i <-0 to 10){
      var output = ""
      output += DateTimeGenerator.generate + ","
      output += quantityTXN.generate + ","
      output += paymentTXN.generate
      Thread.sleep(100)
      println(output)
    }
  }
}
