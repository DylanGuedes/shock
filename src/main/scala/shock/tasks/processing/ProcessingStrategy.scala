package shock.tasks.processing

import shock.Pipeline
import shock.engines.SparkEngine
import play.api.libs.json.{JsValue}

trait ProcessingStrategy {
  def process(engine: SparkEngine, pipeline: Pipeline, opts: JsValue): Pipeline
}
