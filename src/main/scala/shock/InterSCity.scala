// Package: --packages org.mongodb.spark:mongo-spark-connector_2.11:2.2.0

import com.mongodb.spark._
import com.mongodb.spark.config._

object InterSCity {
  def load_database_config(spark): Unit = {
    val readConfig = ReadConfig(Map("collection" -> "sensor_values",
      "spark.mongodb.input.uri" -> "mongodb://data-collector-mongo:27017",
      "spark.mongodb.output.uri" -> "mongodb://dat a-collector-mongo:27017",
      "spark.mongodb.input.database" -> "data_collector_development"))

    val rdd = MongoSpark.load(sc, readConfig)
  }
}
