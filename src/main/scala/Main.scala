import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{IntegerSerializer, StringSerializer}


object Main {
  def main(args: Array[String]): Unit = {

    val topicName = "hadoop_elephants"

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

    var i = 0
    while (true) {
      i += 1
      val output = Generator.generate()
      producer.send(new ProducerRecord[Int, String](topicName, i, output))
      println(output)
      Thread.sleep(2000) // Wait 2 seconds
    }
    producer.flush()
  }
}
