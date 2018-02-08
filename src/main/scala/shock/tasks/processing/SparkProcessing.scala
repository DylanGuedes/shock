package shock.tasks.processing

import shock.engines.{SparkEngine}
import shock.{Pipeline}
import play.api.libs.json.{JsValue}

object WhereQuery extends ProcessingStrategy {
  def process(engine: SparkEngine, pipeline: Pipeline, opts: JsValue): Pipeline = {
    val query: String = (opts \ "query").get.as[String]
    pipeline.state.where(query)
    pipeline
  }
}
