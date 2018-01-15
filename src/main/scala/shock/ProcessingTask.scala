// object ProcessingTask {
//   def perform() = {
//     val df: Dataset[Row] = engine.ingest()
//
//     // val analytics = df.describe()
//     val analytics = df
//     analytics.show()
//     analytics.printSchema()
//     // analytics.toJSON
//     // println("analytics => ", analytics)
//
//     var conn: WS = new WS().open("ws://"+options("--ws-server")+"/socket/websocket")
//     conn = conn.subscribe("analytics")
//
//     var c: String = "temperature"
//     var values: Array[Any] = analytics.select("temperature", "created_at", "humidity").na.drop().collect().map(r => {
//       Map("temperature" -> r(0), "created_at" -> r(1), "humidity" -> r(2))
//     })
//
//     var values2: Array[JsObject] = values.map((item) => {
//       val myhash: Map[String, Any] = item.asInstanceOf[Map[String, Any]]
//       Json.obj(
//         "temperature" -> JsNumber(myhash("temperature").asInstanceOf[Double]),
//         "created_at" -> JsString(myhash("created_at").toString),
//         "humidity" -> JsNumber(myhash("humidity").asInstanceOf[Double])
//       )
//     })
//     var payload = Json.obj(c -> JsArray(values2))
//     conn = conn.sendMsg("analytics", payload, "summary:"+c)
//   }
// }
