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
import shock.{Pipeline}
import play.api.libs.json.{JsValue}
import com.mongodb.spark.MongoSpark

object MongoIngestion extends IngestionStrategy {
  def ingest(engine: SparkEngine, pipeline: Pipeline, opts: JsValue): Pipeline = {
    pipeline.state = Some(MongoSpark.load(engine.sc).toDF())

    pipeline.state match {
      case Some(df) => {
        df.show()
      }

      case None => {
        throw new Exception("The data ingestion didn't work.")
      }
    }

    pipeline
  }
}
