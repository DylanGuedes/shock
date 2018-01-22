package shock.engines.spark

import com.mongodb.spark._
import org.bson.Document
import com.mongodb.spark.config._
import com.mongodb.spark.sql._

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SparkSession, Dataset}
import org.apache.spark.sql.SparkSession.Builder
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.rdd.RDD

class SparkEngine() {
  val conf: SparkConf = new SparkConf().setAppName("Shock").setMaster("local")
  val sc: SparkContext = new SparkContext(this.conf)
  val dbName: String = "data_collector_development"
  val collectionName: String = "sensor_values"
  val ip: String = "data-collector-mongo"
  val port: String = "27017"
  val mongoHost: String = "mongodb://"+ip+":"+port+"/"+dbName+"."+collectionName
  val spark: SparkSession = SparkSession.builder()
    .config("spark.mongodb.input.uri", mongoHost)
    .config("spark.mongodb.output.uri", mongoHost)
    .config("spark.cleaner.referenceTracking.cleanCheckpoints", "true")
    .getOrCreate()
}

