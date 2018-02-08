package shock.tasks.ingestion

import shock.engines.{SparkEngine}
import shock.{Pipeline}
import play.api.libs.json.{JsValue}
import scalaj.http.{HttpResponse, Http}
import java.io.{File, PrintWriter}
import org.apache.spark.sql.SQLContext

object PostRequestIngestion extends IngestionStrategy {
  def ingest(engine: SparkEngine, pipeline: Pipeline, opts: JsValue): Pipeline = {
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
