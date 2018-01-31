package shock.tasks

import org.apache.spark.ml.feature.VectorAssembler
import shock.pipeline.Pipeline
import shock.engines.spark.SparkEngine
import play.api.libs.json.{JsValue}
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row

object ProcessingStrategies {
  def sciPopulisProcessing(engine: SparkEngine, pipeline: Pipeline, opts: JsValue): Pipeline = {
    import engine.spark.implicits._
    val df = pipeline.state
      // .select(explode(col("resources")) as "measures")
    df.show()

    df.printSchema()

    println("resources => ")
    df.select("resources").show()
    df.select("resources").printSchema()

    println("resources.capabilities => ")
    df.select("resources.capabilities").show()
    df.select("resources.capabilities").printSchema()

    println("resources.capabilities.weather.temperature => ")
    val df2 = df.select(explode($"resources.capabilities.weather" as "weather")).na.drop()
    df2.show(20)
    df2.printSchema()

    val df3 = df2.select("col.temperature")
    pipeline.state = df
    pipeline
  }
}
