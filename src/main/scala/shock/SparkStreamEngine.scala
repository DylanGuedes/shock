package shock.engines.spark

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import shock.types.OperationStatusType._
import shock.streaming._

class SparkStreamEngine(ingestionStrategy: IngestionStrategy) extends StreamEngine {
  var conf: SparkConf = null
  var sc: SparkContext = null
  var spark: SparkSession = null

  def setup(): Unit = {
    this.conf = new SparkConf().setAppName("Shock")
    this.conf.setMaster("local")
    this.sc = new SparkContext(conf)
    this.spark = SparkSession.builder.appName("Shock").getOrCreate()
  }

  def teardown(): Unit = {
  }
}

class KafkaStructuredStreaming() extends IngestionStrategy {
}
