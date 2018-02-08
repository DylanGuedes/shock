package shock.tasks.ingestion

import shock.engines.{SparkEngine}
import shock.{Pipeline}
import play.api.libs.json.{JsValue}
import com.mongodb.spark.MongoSpark

object MongoIngestion extends IngestionStrategy {
  def ingest(engine: SparkEngine, pipeline: Pipeline, opts: JsValue): Pipeline = {
    pipeline.state = MongoSpark.load(engine.sc).toDF()

    pipeline.state.show()
    pipeline
  }
}
