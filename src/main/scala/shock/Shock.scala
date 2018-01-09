package shock.main
import org.apache.spark.sql.types._

import org.bson.Document
import shock.engines.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.Dataset

import play.api.libs.json.Json
import play.api.libs.json.JsValue

import org.jfarcand.wcs.{WebSocket, TextListener}

object Shock {
  def main(args: Array[String]) {
    val options: Map[String, String] = parseOptions(args)
    checkRequiredOptions(options)

    val engine = new SparkEngine(new MongoIngestionStrategy(options))
    engine.setup()

    val df: Dataset[Row] = engine.ingest()

    val conn: WebSocket = openWebSocket(options)

    joinTopic(conn, "awesome")
  }

  def openWebSocket(options: Map[String, String]): WebSocket = {
    WebSocket().open("ws://"+options("--ws-server")+"/socket/websocket")
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

  def joinTopic(conn: WebSocket, topic: String): WebSocket = {
    sendMsg(conn, topic, Json.obj(), "phx_join")
  }

  def sendMsg(conn: WebSocket, topic: String, payload: JsValue, event: String = "payload"): WebSocket = {
    var msg: JsValue = Json.obj(
      "topic" -> topic,
      "event" -> event,
      "payload" -> Json.stringify(payload),
      "ref" -> ""
    )

    conn.send(Json.stringify(msg))
  }
}
