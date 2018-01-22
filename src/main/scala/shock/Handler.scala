package shock.handlers

import shock.pipeline.Pipeline
import shock.engines.spark.{SparkEngine}
import shock.tasks.{IngestionStrategies, PublishingStrategies}
import shock.aliases.{TaskSignature, StringHash}

trait Handler {
  def handle(msg: String): Unit
  def loadResolvers(): Unit
}

class InterSCityHandler(options: StringHash) extends Handler {
  var engine: SparkEngine = new SparkEngine()
  var pipelines: Map[String, Pipeline] = Map[String, Pipeline]()
  var resolvers: Map[String, TaskSignature] = Map[String, TaskSignature]()

  def loadResolvers(): Unit = {
    this.resolvers += ("mongo_ingestion" -> IngestionStrategies.mongoIngestion)
    this.resolvers += ("websocket_publish" -> PublishingStrategies.websocketPublishing)
  }

  def handle(msg: String) {
    val tokens: Array[String] = msg.split(";")

    if (tokens.length == 3) {
      val pipelineName: String = tokens(0)
      val taskMethod: String = tokens(1)
      val taskArgs: String = tokens(2)

      if (!pipelines.keySet.exists(_ == pipelineName))
        pipelines += (pipelineName -> new Pipeline())

      // taskArgs format: opt1=val1,opt2=val2,opt3=val3,...
      val opts: StringHash = taskArgs.split(",").foldLeft(Map[String, String]())((total: StringHash, item: String) => {
        val splittedArg: Array[String] = item.split("=")
        total + (splittedArg(0) -> splittedArg(1))
      })
      
      pipelines(pipelineName).addTask(this.resolvers(taskMethod), opts)
    } else if (tokens.length == 2) {
      val command: String = tokens(0)
      val pipelineName: String = tokens(1)

      if (pipelines.keySet.exists(_ == pipelineName)) {
        pipelines(pipelineName).start(engine)
      }
    }
  }
}
