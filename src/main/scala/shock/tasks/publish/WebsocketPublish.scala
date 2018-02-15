package shock.tasks.publish

import org.apache.spark.sql.{Dataset, Row}

import shock.websocket.WS
import shock.Pipeline
import shock.engines.SparkEngine
import play.api.libs.json.{JsValue, JsObject, Json, JsNumber, JsArray}
import org.apache.spark.sql.functions.{col, explode, desc}

object WebsocketPublish extends PublishStrategy {
  def publish(engine: SparkEngine, pipeline: Pipeline, opts: JsValue): Pipeline = {
    // check if the state is empty
    if (pipeline.state.count != 0) {
      val df: Dataset[Row] = pipeline.state

      var ws_server = "ws://172.17.0.1:41234/socket/websocket"

      val opts2: scala.collection.Map[String, JsValue]= opts.as[JsObject].value

      if (opts2.keySet.exists(_ == "ws_server")) {
        ws_server = opts2("ws_server").as[String]
      }

      var conn: WS = new WS().open(ws_server).subscribe("analytics")

      var ws_topic = "analytics"
      if (opts2.keySet.exists(_ == "ws_topic")) {
        ws_topic = opts2("ws_topic").as[String]
      }
      conn = conn.subscribe(ws_topic)

      df.printSchema()

      val features: Seq[String] = opts2("features").as[Seq[String]]
      // var values: Dataset[Row] = df.select(features.map(c => col(c)): _*)
      var values: Dataset[Row] = df.select("temperature", "created_at", "humidity")

      var dropNil: Boolean = true
      if (opts2.keySet.exists(_ == "drop_nil")) {
        dropNil = opts("drop_nil").as[String].toBoolean
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
      if (opts2.keySet.exists(_ == "update")) {
        ws_event = opts("update").as[String]
      }

      var payload = Json.obj(opts("result_feature").as[String] -> JsArray(values2))
      conn = conn.sendMsg(ws_topic, payload, ws_event)
    }
    pipeline
  }
}
