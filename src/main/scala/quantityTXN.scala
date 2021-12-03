

object quantityTXN {
  var theCount = 0  // counts # of times called, hopefully // used to generate trends ie. after a while, our business will grow
  var theCountess :Array[Int] = new Array(100)
  val theWarehouse = Array("Camera Accessories", "Car Electronics & GPS", "Cell Phones & Accessories", "Computers & Accessories", "Digital Cameras", "Hard Drives", "Headphones", "Home Audio", "Home Security", "Media Players & Recorders", "Memory Cards", "Mirrorless System Lenses", "Portable Bluetooth Speakers", "Power Supplies", "Projection Screens & Material", "Security Cameras", "Smart Watches", "Solid State Drives", "Speaker Cables", "Speaker Systems", "Streaming Media Players", "TV", "TV Accessories & Parts", "Video Games", "Wireless Speakers")
  val rando = new scala.util.Random(System.currentTimeMillis)
  var i = 0

  // repeated calls without any parameters will increase the number returned by this function
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

  // parameter of type boolean will just ignore the counters
  //    works for true or false
  def generate(killTrend: Boolean): String ={
    i = 0
    this.i = this.rando.nextInt(1000)
    return i.toString
  }

  //  separate counters for each category type --declared up top--
  def generate(category: String): String ={
    val ID = this.theWarehouse.indexOf(category)
    if(ID < 0){
      return "Error: Category Not Found"
    }
    this.theCountess(ID) += 1 // instead of generic count
    i = 0
    i = this.rando.nextInt(100)
    if(this.theCountess(ID) >= 10){ // insert various conditions here
      i += 100
    }
    return i.toString
  }
}
