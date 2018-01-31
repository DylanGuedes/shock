package shock.tasks

import shock.pipeline.Pipeline
import shock.engines.spark._
import scalaj.http.{HttpResponse, Http}

import com.mongodb.spark.MongoSpark
import play.api.libs.json.{JsValue, Json}
import java.io.File
import java.io.PrintWriter

import org.apache.spark.sql.SQLContext
import play.api.libs.json._

object IngestionStrategies {
  def mongoIngestion(engine: SparkEngine, pipeline: Pipeline, opts: JsValue): Pipeline = {
    pipeline.state = MongoSpark.load(engine.sc).toDF()

    pipeline.state.show()
    pipeline
  }

  def postRequestIngestion(engine: SparkEngine, pipeline: Pipeline, opts: JsValue): Pipeline = {
    val response: String = Http("http://data-collector:3000/resources/data").asString.body

    val writer = new PrintWriter(new File("/data/interscity.json"))
    writer.write(response)
    writer.close()

    val sqlContext = SQLContext.getOrCreate(engine.sc)
    import sqlContext.implicits._
    import sqlContext._

    val df = engine.spark.read.json("/data/interscity.json")
    pipeline.state = df
    pipeline.state.show()
    pipeline
  }
}
