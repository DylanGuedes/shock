package shock.pipeline

import org.apache.spark.sql.DataFrame

class Pipeline() {
  var state: DataFrame = null
  var tasksQueue: Queue[() => Unit] = Queue[() => Unit]()
}
