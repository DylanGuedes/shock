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

package shock

import shock.aliases.TaskSignature
import shock.engines.SparkEngine

import org.apache.spark.sql.DataFrame
import org.apache.spark.SparkContext

import scala.collection.mutable.Queue
import play.api.libs.json.{JsValue, Json}

class Pipeline() {
  var state: Option[DataFrame] = None: Option[DataFrame]
  var tasksQueue: Queue[(TaskSignature, JsValue)] = Queue[(TaskSignature, JsValue)]()
  def addTask(task: TaskSignature, args: JsValue): Unit = {
    this.tasksQueue += (task, args).asInstanceOf[Tuple2[TaskSignature, JsValue]]
  }

  def start(se: SparkEngine): Pipeline = {
    println("starting...")
    start(se, this)
  }

  def start(se: SparkEngine, pipeline: Pipeline): Pipeline = {
    tasksQueue.foldLeft(pipeline: Pipeline)((acc: Pipeline, pair: (TaskSignature, JsValue)) => {
      val (task: TaskSignature, args: JsValue) = pair
      Json.prettyPrint(args)
      task(se, acc, args)
    })
  }
}
