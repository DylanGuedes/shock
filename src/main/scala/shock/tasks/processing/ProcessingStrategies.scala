package shock.tasks.processing

import shock.Pipeline
import shock.engines.SparkEngine
import play.api.libs.json.{JsValue}
import org.apache.spark.sql.functions.{col, explode, desc}
import org.apache.spark.sql.{Row, SQLContext}

import scala.collection.mutable.WrappedArray

object ProcessingStrategies {
  def sciPopulisProcessing(engine: SparkEngine, pipeline: Pipeline, opts: JsValue): Pipeline = {
    import engine.spark.implicits._
    val df = pipeline.state
    df.show()

    df.printSchema()

    df.select("resources").show()
    df.select("resources").printSchema()

    df.select("resources.capabilities").show()
    df.select("resources.capabilities").printSchema()

    val df2 = df.select(explode($"resources.capabilities.edge_monitoring") as "edge_monitoring").na.drop()
    df2.show(20)
    df2.printSchema()

    val df3 = df2.select("edge_monitoring.avg_speed", "edge_monitoring.origin", "edge_monitoring.destination", "edge_monitoring.date")
    df3.show()

    pipeline.state = df
    pipeline
  }
}
