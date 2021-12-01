object paymentType {
  val payments = Array("Card", "Internet Banking", "UPI", "Wallet")

  def generate(): String ={
    val r = new scala.util.Random(System.currentTimeMillis())
    val x = r.nextInt(100)
    if (x > 20){
      return payments(r.nextInt(2))
    }
    else{
      return payments(r.nextInt(4))
    }

  }
}
