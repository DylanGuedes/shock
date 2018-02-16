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

package shock.handlers

import shock.Pipeline
import shock.engines.{SparkEngine}
import shock.tasks.ingestion.{MongoIngestion, PostRequestIngestion}
import shock.tasks.publish.WebsocketPublish
import shock.tasks.processing.WhereQuery
import shock.aliases.{TaskSignature, StringHash}

import play.api.libs.json.{Json, JsValue}

class InterSCityHandler(options: StringHash) extends Handler {
  var engine: SparkEngine = new SparkEngine()
  var pipelines: Map[String, Pipeline] = Map[String, Pipeline]()
  var resolvers: Map[String, TaskSignature] = Map[String, TaskSignature]()

  def loadResolvers(): Unit = {
    this.resolvers += ("mongo_ingestion" -> MongoIngestion.ingest)
    this.resolvers += ("websocket_publish" -> WebsocketPublish.publish)
    this.resolvers += ("post_request_ingestion" -> PostRequestIngestion.ingest)
    this.resolvers += ("sci_populis_processing" -> WhereQuery.process)
  }

  /*
   * Message example: new_pipeline#{"key": "val"}
   * The value before the sharp is the desired command, the value after
   * the sharp is a JSON hash with options to be used by the command
   *
   * Commands available:
   * 1. "new_pipeline": Register a new pipeline
   * 2. "update_pipeline": Adds tasks to a registered pipeline
   * 3. "info": List every task registered in pipeline
   */
  def handle(msg: String): Unit = {
    val tokens: Array[String] = msg.split("#")

    val opts: JsValue = Json.parse(tokens(1))
    val pipelineName: String = (opts \ "stream").as[String]
    val commandName: String = tokens(0)

    commandName match {
      case "new_pipeline" => {
        if (!pipelines.keySet.exists(_ == pipelineName)) {
          pipelines += (pipelineName -> new Pipeline())
        }
      }

      case "update_pipeline" => {
        val taskMethod: String = (opts \ "shock_action").as[String]

        pipelines(pipelineName).addTask(this.resolvers(taskMethod), opts)
      }

      case "start_pipeline" => {
        handleStartPipeline(pipelineName)
      }

      case "info" => {
        handleInfo(pipelineName)
      }
    }
  }

  def handleInfo(pipelineName: String): Unit = {
    pipelines(pipelineName).tasksQueue.foreach((mtd) => { println(mtd) })
  }

  def handleStartPipeline(pipelineName: String): Unit = {
    if (pipelines.keySet.exists(_ == pipelineName)) {
      pipelines(pipelineName).start(engine)
    }
  }
}
