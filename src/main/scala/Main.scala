import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{IntegerSerializer, StringSerializer}

object Main {

  def main(args: Array[String]): Unit = {

    val topicName = "test-1"

    val producerProperties = new Properties()
    producerProperties.setProperty(
      ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"
    )
    producerProperties.setProperty(
      ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[IntegerSerializer].getName
    )
    producerProperties.setProperty(
      ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName
    )

    val producer = new KafkaProducer[Int, String](producerProperties)

    /*producer.send(new ProducerRecord[Int, String](topicName, 10, "Message 1"))
    producer.send(new ProducerRecord[Int, String](topicName, 20, "Message 2"))
    producer.send(new ProducerRecord[Int, String](topicName, 30, "Message 3"))
    producer.send(new ProducerRecord[Int, String](topicName, 40, "Message 4"))
    producer.send(new ProducerRecord[Int, String](topicName, 50, "Message 5"))*/

  //  producer.flush() //producer.sends works async and just to ake sure alle the messages are published


    var i = 0
    for(i <-0 to 10){
      val test = paymentTXN.generate
     // val webtest = websiteGen.generate
     // val payment = paymentType.generate
     // Thread.sleep(100)
    //  println(test, webtest, payment)

      producer.send(new ProducerRecord[Int, String](topicName, 60, test))

    }
    producer.flush()

  }
}
