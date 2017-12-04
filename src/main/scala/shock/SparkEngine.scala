package shock.engines.spark

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import com.mongodb.spark._
import org.bson.Document
import com.mongodb.spark.config._
import com.mongodb.spark.sql._

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
    this.spark = SparkSession.builder()
      .appName("Shock")
      .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/sp.weather")
      .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/sp.weather")
      .getOrCreate()
  }

  def compute(): Unit = {
    println("SC: ", this.conf)
    val rdd = this.conf.loadFromMongoDB()
    println("RDD: ",rdd)
    println(rdd.count())
    println(rdd.first.toJson())
  }

  def teardown(): Unit = {
  }
}

class MongoIngestionStrategy() extends IngestionStrategy {
}
