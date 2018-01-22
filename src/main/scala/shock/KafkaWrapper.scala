package shock.kafka

import org.apache.kafka.clients.consumer.{
  ConsumerConfig,
  KafkaConsumer,
  ConsumerRecords,
  ConsumerRecord
}
import java.util.{Properties, Collections}
import scala.collection.JavaConversions._
import shock.handlers._

class KC(options: Map[String, String]) {
  val props = createConsumerConfig(options)
  val _kafkaConsumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](props)
  val topic = "pipeline"
  val handler: Handler = new InterSCityHandler(options)

  def createConsumerConfig(options: Map[String, String]): Properties = {
    var brokers: String = "kafka:9092"
    var groupId: String = "shock"

    if (options.keySet.exists(_ == "--bootstrap-servers")) {
      brokers = options("--bootstrap-servers")
    }

    val opts = new Properties()
    opts.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    opts.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    opts.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
    opts.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000")
    opts.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000")
    opts.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringDeserializer")
    opts.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringDeserializer")

    opts
  }

  def consume(pollRate: Long = 1000): Unit = {
    this.handler.loadResolvers()
    println("Loading resolvers...")
    _kafkaConsumer.subscribe(Collections.singletonList(this.topic))
    println("Subscribing to topics...")

    println("Preparing to start fetching...")
    while (true) {
      val records: ConsumerRecords[String, String] = _kafkaConsumer.poll(pollRate)

      for (record: ConsumerRecord[String, String] <- records) {
        val msg: String = record.value
        println("Msg => ", msg)
        this.handler.handle(msg)
      }
    }
  }
}
