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

package shock.tasks.ingestion

import shock.engines.{SparkEngine}
import org.apache.spark.sql.functions.{col, explode}
import shock.{Pipeline}
import play.api.libs.json.{JsValue}
import scalaj.http.{HttpResponse, Http}
import java.io.{File, PrintWriter}
import org.apache.spark.sql.SQLContext

object PostRequestIngestion extends IngestionStrategy {
  def ingest(engine: SparkEngine, pipeline: Pipeline, opts: JsValue): Pipeline = {
    val response: String = Http("http://data-collector:3000/resources/data").asString.body

    val writer = new PrintWriter(new File("/data/interscity.json"))
    writer.write(response)
    writer.close()

    pipeline.state = Some(engine.spark.read.json("/data/interscity.json"))
    pipeline.state = Some(pipeline.state.get.select(explode(col("resources").alias("resources")) as "resources"))

    pipeline.state match {
      case Some(df) => {
        df.printSchema()
        df.show()
      }

      case None => {
        throw new Exception("The data ingestion didn't work.")
      }
    }
    pipeline
  }
}
