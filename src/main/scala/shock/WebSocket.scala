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
  }

  def sendMsg(topic: String, payload: JsValue, event: String = "payload"): WS = {
    val msg: JsValue = Json.obj(
      "topic" -> topic,
      "event" -> event,
      "payload" -> Json.stringify(payload),
      "ref" -> ""
    )

    this._webSocket = this._webSocket.send(Json.stringify(msg))
    this
  }
}
