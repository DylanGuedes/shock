package shock.engines.spark

import com.mongodb.spark._
import org.bson.Document
import com.mongodb.spark.config._
import com.mongodb.spark.sql._

import org.apache.spark.sql.Row
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

import shock.types.OperationStatusType._
import shock.batch._

class SparkEngine(ingestionStrategy: IngestionStrategy) extends BatchEngine {
  var conf: SparkConf = null
  var sc: SparkContext = null
  var spark: SparkSession = null

  def setup(): Unit = {
    this.conf = new SparkConf().setAppName("Shock")
    this.conf.setMaster("local")
    this.sc = new SparkContext(conf)

    val dbName = "data_collector_development"
    val collectionName = "sensor_values"
    val ip = "data-collector-mongo"
    val port = "27017"
    val host = "mongodb://"+ip+":"+port+"/"+dbName+"."+collectionName

    this.spark = SparkSession.builder()
      .appName("Shock")
      .config("spark.mongodb.input.uri", host)
      .config("spark.mongodb.output.uri", host)
      .getOrCreate()
  }

  def ingest(): Dataset[Row] = {
    MongoSpark.load(sc).toDF()
  }

  def teardown(): Unit = {
  }
}

class MongoIngestionStrategy() extends IngestionStrategy {
}
