package shock.main

import shock.kafka.KC

import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}

import java.util.{Collections, Properties}

object Shock {
  var options: Map[String, String] = Map[String, String]()

  def main(args: Array[String]) {
    this.options = parseOptions(args)
    checkRequiredOptions(options)

    val consumer: KC = new KC(options)
    println("Start consuming...")
    consumer.consume()
  }

  def parseOptions(args: Array[String]): Map[String, String] = {
    val splittedArgs: List[Array[String]] = args.sliding(2, 2).toList
    splittedArgs.foldLeft(Map[String, String]())(
      (t: Map[String, String], opt: Array[String]) => t + (opt(0) -> opt(1))
    )
  }

  def checkRequiredOptions(options: Map[String, String]): Unit = {
    if (!options.keySet.exists(_ == "--ws-server")) {
      // websocket server used to build the pipeline
      throw new RuntimeException("Missing required param: ws-server")
    }
  }
}
