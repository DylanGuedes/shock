package shock.pipeline

import shock.aliases.{StringHash, TaskSignature}
import shock.engines.spark.SparkEngine

import org.apache.spark.sql.DataFrame

import scala.collection.mutable.Queue

class Pipeline() {
  var state: DataFrame = null
  var tasksQueue: Queue[(TaskSignature, StringHash)] = Queue[(TaskSignature, StringHash)]()
  def addTask(task: TaskSignature, args: StringHash): Unit = {
    this.tasksQueue += (task, args).asInstanceOf[Tuple2[TaskSignature, StringHash]]
  }

  def start(se: SparkEngine): Pipeline = {
    start(se, this)
  }

  def start(se: SparkEngine, pipeline: Pipeline): Pipeline = {
    tasksQueue.foldLeft(pipeline: Pipeline)((acc: Pipeline, pair: (TaskSignature, StringHash)) => {
      val (task: TaskSignature, args: StringHash) = pair
      task(se, acc, args)
    })
  }
}
