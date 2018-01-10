package shock.main
import org.apache.spark.sql.types._

import shock.websocket.WS

import org.bson.Document
import shock.engines.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.Dataset

import org.apache.spark.sql.functions.{col}

import play.api.libs.json.{JsString, Json}

object Shock {
  def main(args: Array[String]) {
    val options: Map[String, String] = parseOptions(args)
    checkRequiredOptions(options)

    val engine = new SparkEngine(new MongoIngestionStrategy(options))
    engine.setup()

    val df: Dataset[Row] = engine.ingest()
    val analytics = df.describe()
    analytics.show()
    // analytics.printSchema()
    // analytics.toJSON
    // println("analytics => ", analytics)

    val conn: WS = new WS().open("ws://"+options("--ws-server")+"/socket/websocket")

    conn.subscribe("analytics")
    analytics.columns.map({c =>
      val values: List[Any] = analytics.select(col(c)).collect().map(r => r(0)).toList
      val payload = Json.obj(
        "count" -> JsString(values(0).asInstanceOf[String]),
        "mean" -> JsString(values(1).asInstanceOf[String]),
        "stddev" -> JsString(values(2).asInstanceOf[String]),
        "min" -> JsString(values(3).asInstanceOf[String]),
        "max" -> JsString(values(4).asInstanceOf[String])
      )

      conn.sendMsg("analytics:summary:"+c, payload)
    })
  }

  def parseOptions(args: Array[String]): Map[String, String] = {
    val splittedArgs: List[Array[String]] = args.sliding(2, 2).toList
    splittedArgs.foldLeft(Map[String, String]())(
      (t: Map[String, String], opt: Array[String]) => t + (opt(0) -> opt(1))
    )
  }

  def checkRequiredOptions(options: Map[String, String]): Unit = {
    if (!options.keySet.exists(_ == "--ws-server")) {
      // websocket server used to build the pipeline
      throw new RuntimeException("Missing required param: ws-server")
    }
  }
}
