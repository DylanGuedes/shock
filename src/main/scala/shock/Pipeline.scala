package shock.pipeline

import shock.aliases.TaskSignature
import shock.engines.spark.SparkEngine

import org.apache.spark.sql.DataFrame

import scala.collection.mutable.Queue
import play.api.libs.json.{JsValue, Json}

class Pipeline() {
  var state: DataFrame = null
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
      println("args => ")
      Json.prettyPrint(args)
      task(se, acc, args)
    })
  }
}
