class CSV {
  //making the header




  def writeHeader() = {
    val str  = "order_id," +
      "customer_id," +
      "customer_name," +
      "product_id," +
      "product_name," +
      "product_category," +
      "payment_type," +
      "qty," +
      "price," +
      "datetime," +
      "country," +
      "city," +
      "ecommerce_website_name," +
      "payment_txn_id," +
      "payment_txn_success," +
      "failure_reason"

    import java.io._
    var pw = new PrintWriter(new FileOutputStream(
      new File("Stream.csv"),
      false /* append = true */));
    pw.write(str+"\n" )
    pw.close
  }





  def append(str: String) = {
    import java.io._
    var pw = new PrintWriter(new FileOutputStream(
      new File("Stream.csv"),
      true /* append = true */));
    pw.write(str+" \n" )
    pw.close
  }}