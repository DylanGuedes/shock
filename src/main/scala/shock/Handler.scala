package shock.handlers

import shock.pipeline.Pipeline
import shock.engines.spark.{SparkEngine}
import shock.tasks.{IngestionStrategies, PublishingStrategies, ProcessingStrategies}
import shock.aliases.{TaskSignature, StringHash}

import play.api.libs.json.{Json, JsValue}

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
    this.resolvers += ("post_request_ingestion" -> IngestionStrategies.postRequestIngestion)
    this.resolvers += ("anomaly_detection" -> IngestionStrategies.postRequestIngestion)
    this.resolvers += ("post_publish" -> IngestionStrategies.postRequestIngestion)
    this.resolvers += ("sci_populis_processing" -> ProcessingStrategies.sciPopulisProcessing)
  }

  def handle(msg: String) {
    val tokens: Array[String] = msg.split("#")

    val opts: JsValue = Json.parse(tokens(1))
    val pipelineName: String = (opts \ "stream").as[String]

    if (tokens(0) == "new_pipeline") {
      if (!pipelines.keySet.exists(_ == pipelineName))
        pipelines += (pipelineName -> new Pipeline())
    } else if (tokens(0) == "update_pipeline") {
      val taskMethod: String = (opts \ "shock_action").as[String]

      pipelines(pipelineName).addTask(this.resolvers(taskMethod), opts)
    } else if (tokens(0) == "start_pipeline") {
      if (pipelines.keySet.exists(_ == pipelineName)) {
        pipelines(pipelineName).start(engine)
      }
    } else if (tokens(0) == "info") {
      pipelines(pipelineName).tasksQueue.foreach((mtd) => { println(mtd) })
    }
  }
}
