package shock.tasks

import shock.pipeline.Pipeline
import shock.engines.spark._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SparkSession, Dataset}

import play.api.libs.json.{Json, JsValue, JsArray, JsNumber, JsObject}

import shock.websocket.WS

object PublishingStrategies {
  def websocketPublishing(engine: SparkEngine, pipeline: Pipeline, opts: Map[String, String]): Pipeline = {
    if (!pipeline.state.head(1).isEmpty) {
      val df: Dataset[Row] = pipeline.state

      var ws_server = "ws://172.17.0.1:41234/socket/websocket"
      if (opts.keySet.exists(_ == "ws_server")) {
        ws_server = opts("ws_server")
      }
      var conn: WS = new WS().open(ws_server).subscribe("analytics")

      var ws_topic = "analytics"
      if (opts.keySet.exists(_ == "ws_topic")) {
        ws_topic = opts("ws_topic")
      }
      conn = conn.subscribe(ws_topic)

      df.printSchema()

      val features: Seq[String] = opts("features").split(" . ")
      // var values: Dataset[Row] = df.select(features.map(c => col(c)): _*)
      var values: Dataset[Row] = df.select("temperature", "created_at", "humidity")

      var dropNil: Boolean = true
      if (opts.keySet.exists(_ == "drop_nil")) {
        dropNil = opts("drop_nil").toBoolean
      }

      if (dropNil) {
        values = values.na.drop()
      }

      val cleanValues: Array[Any] = values.collect().map((r: Row) => {
        r.getValuesMap(features)
      })

      var values2: Array[JsObject] = cleanValues.map((item) => {
        val myhash: Map[String, Any] = item.asInstanceOf[Map[String, Any]]
        features.foldLeft(Json.obj())((total, item) => {
          total + (item -> JsNumber(myhash(item).asInstanceOf[Double]))
        })
      })

      var ws_event = "update"
      if (opts.keySet.exists(_ == "update")) {
        ws_event = opts("update")
      }

      var payload = Json.obj(opts("result_feature") -> JsArray(values2))
      conn = conn.sendMsg(ws_topic, payload, ws_event)
    }
    pipeline
  }
}
