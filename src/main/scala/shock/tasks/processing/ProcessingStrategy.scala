package shock.tasks.processing

import shock.Pipeline
import shock.engines.SparkEngine
import play.api.libs.json.{JsValue}
import org.apache.spark.sql.functions.{col, explode, desc}
import org.apache.spark.sql.{Row, SQLContext}

import scala.collection.mutable.WrappedArray

trait ProcessingStrategy {
  def process(engine: SparkEngine, pipeline: Pipeline, opts: JsValue): Pipeline
}
