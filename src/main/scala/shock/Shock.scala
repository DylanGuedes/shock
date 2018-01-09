package shock.main
import org.apache.spark.sql.types._

import org.bson.Document
import shock.engines.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.Dataset

import play.api.libs.json.Json

import org.jfarcand.wcs.{WebSocket, TextListener}

object Shock {
  def main(args: Array[String]) {
    val engine = new SparkEngine(new MongoIngestionStrategy)
    engine.setup()

    val df: Dataset[Row] = engine.ingest()
    // val schema = StructType(StructField("name", StringType))
    // val df = engine.spark.createDataFrame(rdd)
    // val df = rdd.toDF()
    // println("SCHEMA=>")
    println("count now=> ", df.count())
    // val df2 = df.filter("capability == 'open' OR capability == 'slots_monitoring'")
    println("now now=>", df.count())
    df.printSchema()
    val df3 = df.select("bike_slots").filter("bike_slots is not null")
    println(df3.show())
    val statistics = df3.describe().selectExpr("summary as key", "bike_slots as value")
    statistics.show()

    WebSocket().open("ws://172.17.0.1:41234/socket/websocket").listener(
      new TextListener {
        override def onMessage(message: String) {
          println(message)
        }
      }).send(Json.stringify(Json.obj("name" -> "hi")))
  }
}
