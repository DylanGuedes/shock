package shock.tasks.ingestion

import shock.Pipeline
import shock.engines.SparkEngine
import com.mongodb.spark.MongoSpark
import play.api.libs.json.{JsValue}

trait IngestionStrategy {
  def ingest(engine: SparkEngine, pipeline: Pipeline, opts: JsValue): Pipeline
}
