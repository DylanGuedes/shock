package shock.handlers

import shock.pipeline.Pipeline
import shock.engines.spark._
import shock.tasks._

trait Handler {
  def handle(msg: String): Unit
  def loadResolvers(): Unit
}

class InterSCityHandler(options: Map[String, String]) extends Handler {
  var engine: SparkEngine = new SparkEngine()
  var pipelines: Map[String, Pipeline] = Map[String, Pipeline]()
  var resolvers: Map[String, (SparkEngine, Pipeline, String) => Pipeline] = Map[String, (SparkEngine, Pipeline, String) => Pipeline]()

  def loadResolvers(): Unit = {
    this.resolvers += ("mongo_ingestion" -> IngestionStrategies.mongoIngestion)
  }

  def handle(msg: String) {
    val tokens: Array[String] = msg.split(";")

    if (tokens.length == 3) {
      val pipelineName: String = tokens(0)
      val taskMethod: String = tokens(1)
      val taskArgs: String = tokens(2)

      if (!pipelines.keySet.exists(_ == pipelineName))
        pipelines += (pipelineName -> new Pipeline())

      pipeline.
      resolvers(taskMethod)(engine, pipelines(pipelineName), taskArgs)
    } else {
      println("Wrong number of tokens!")
    }
  }
}
