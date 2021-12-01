object Main {

  def main(args: Array[String]): Unit = {
    for (_ <- 0 to 10) {
      println(Generator.generate())
      Thread.sleep(100)
    }
  }
}
