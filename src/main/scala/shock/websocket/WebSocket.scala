// Copyright (C) 2018 Dylan Guedes
//
// This file is part of Shock.
//
// Shock is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Shock is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with Shock. If not, see <http://www.gnu.org/licenses/>.

package shock.websocket

import org.jfarcand.wcs.{WebSocket, TextListener}
import play.api.libs.json.{Json, JsValue}

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
