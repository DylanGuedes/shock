package shock.tasks.publish

import shock.Pipeline
import shock.engines.SparkEngine
import play.api.libs.json.{JsValue}
import org.apache.spark.sql.functions.{col, explode, desc}

trait PublishStrategy {
  def publish(engine: SparkEngine, pipeline: Pipeline, opts: JsValue): Pipeline
}
