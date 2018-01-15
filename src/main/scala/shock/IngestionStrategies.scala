package shock.tasks

import shock.pipeline.Pipeline
import shock.engines.spark._

import com.mongodb.spark.MongoSpark

object IngestionStrategies {
  def mongoIngestion(engine: SparkEngine, pipeline: Pipeline, args: String): Pipeline = {
    pipeline.state = MongoSpark.load(engine.sc).toDF()

    pipeline.state.show()
    pipeline
  }
}
