package shock.engines.spark

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import shock.types.OperationStatusType._
import shock.streaming._

class SparkStreamEngine extends StreamEngine {
  var conf: SparkConf = null
  var sc: SparkContext = null

  def setup(): Unit = {
    this.conf = new SparkConf().setAppName("Shock")
    this.conf.setMaster("local")
    this.sc = new SparkContext(conf)
  }

  def teardown(): Unit = {
  }
}
