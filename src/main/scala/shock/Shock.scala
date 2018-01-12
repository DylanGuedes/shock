package shock.main
import org.apache.spark.sql.types._

import shock.websocket.WS

import org.bson.Document
import shock.engines.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.Dataset

import org.apache.spark.sql.functions.{col}

import play.api.libs.json.{JsString, Json, JsArray, JsNumber, JsObject}

object Shock {
  def main(args: Array[String]) {
    val options: Map[String, String] = parseOptions(args)
    checkRequiredOptions(options)

    val engine = new SparkEngine(new MongoIngestionStrategy(options))
    engine.setup()

    val df: Dataset[Row] = engine.ingest()
    // val analytics = df.describe()
    val analytics = df
    analytics.show()
    analytics.printSchema()
    // analytics.toJSON
    // println("analytics => ", analytics)

    var conn: WS = new WS().open("ws://"+options("--ws-server")+"/socket/websocket")
    conn = conn.subscribe("analytics")

    var c: String = "temperature"
    var values: Array[Any] = analytics.select("temperature", "created_at", "humidity").na.drop().collect().map(r => {
      Map("temperature" -> r(0), "created_at" -> r(1), "humidity" -> r(2))
    })

    var values2: Array[JsObject] = values.map((item) => {
      val myhash: Map[String, Any] = item.asInstanceOf[Map[String, Any]]
      Json.obj(
        "temperature" -> JsNumber(myhash("temperature").asInstanceOf[Double]),
        "created_at" -> JsString(myhash("created_at").toString),
        "humidity" -> JsNumber(myhash("humidity").asInstanceOf[Double])
      )
    })
    var payload = Json.obj(c -> JsArray(values2))
    conn = conn.sendMsg("analytics", payload, "summary:"+c)

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
