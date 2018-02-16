// Copyright (C) 2018 Dylan Guedes
//
// This file is part of Shock.
//
// Shock is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Shock is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with Shock. If not, see <http://www.gnu.org/licenses/>.

package shock

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
  val topic = "new_pipeline_instruction"
  val handler: Handler = new InterSCityHandler(options)
  val recommendedValue: Double = 1000

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

  def consume(pollRate: Long = recommendedValue): Unit = {
    this.handler.loadResolvers()
    println("Loading resolvers...")
    _kafkaConsumer.subscribe(Collections.singletonList(this.topic))
    println("Subscribing to topics...")

    println("Preparing to start fetching...")
    while (true) {
      val records: ConsumerRecords[String, String] = _kafkaConsumer.poll(pollRate)

      for (record: ConsumerRecord[String, String] <- records) {
        val msg: String = record.value
        println("Msg => " msg)
        this.handler.handle(msg)
      }
    }
  }
}
