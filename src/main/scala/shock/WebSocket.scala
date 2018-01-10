package shock.websocket

import org.jfarcand.wcs.{WebSocket, TextListener}
import play.api.libs.json.Json
import play.api.libs.json.JsValue

class WS() {
  var _webSocket: WebSocket = WebSocket()

  def open(host: String): WS = {
    this._webSocket = this._webSocket.open(host)
    this
  }

  def subscribe(topic: String): WS = {
    sendMsg(topic, Json.obj(), "phx_join")
    this
  }

  def sendMsg(topic: String, payload: JsValue, event: String = "payload"): WS = {
    var msg: JsValue = Json.obj(
      "topic" -> topic,
      "event" -> event,
      "payload" -> Json.stringify(payload),
      "ref" -> ""
    )

    this._webSocket.send(Json.stringify(msg))
    this
  }

  def on(event: String, callback: () => Unit) {
    println("event happened..")
    this._webSocket.listener(new TextListener {
      override def onMessage(message: String) {
        println("message => ", message)
      }
    })
  }
}
