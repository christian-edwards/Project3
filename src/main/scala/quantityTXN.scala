

object quantityTXN {
  var theCount = 0  // counts # of times called, hopefully // used to generate trends ie. after a while, our business will grow
  var theCountess :Array[Int] = new Array(100)
  val theWarehouse = Array("VCR", "SSD", "Graphics Card", "Smart Phone")
  val rando = new scala.util.Random(System.currentTimeMillis)
  var i = 0

  def generate(): String ={ //generate with basic trending
    this.theCount += 1
    i = 0
    if(this.theCount > 25){ // end of our trend
      this.i  = this.rando.nextInt(250) + 250 // upToRoof + floor
    } else if(this.theCount > 10){ // small number
      this.i  = this.rando.nextInt(150) + 100
    } else { // smallest number
      this.i = this.rando.nextInt(100)
    }
    return i.toString
  }

  def generate(killTrend: Boolean): String ={
    this.theCount += 1
    i = 0
    this.i = this.rando.nextInt(1000)
    return i.toString
  }

  def generate(category: String): String ={
    val ID = this.theWarehouse.indexOf(category)
    if(ID < 0){
      return "WTF was that again?"
    }
    this.theCountess(ID) += 1 // instead of generic count
    i = 0
    i = this.rando.nextInt(100)
    if(this.theCountess(ID) >= 10){
      i += 100
    }
    return i.toString
  }
}
