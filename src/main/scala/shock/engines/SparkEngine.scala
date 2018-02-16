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

package shock.engines

import com.mongodb.spark._
import org.bson.Document
import com.mongodb.spark.config._
import com.mongodb.spark.sql._

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SQLContext, Row, SparkSession, Dataset}
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
  val mongoHost: String = "mongodb://" + ip + ":" + port + "/" + dbName + "." + collectionName
  val spark: SparkSession = SparkSession.builder()
    .config("spark.mongodb.input.uri", mongoHost)
    .config("spark.mongodb.output.uri", mongoHost)
    .config("spark.sql.warehouse.dir", "/data")
    .config("spark.cleaner.referenceTracking.cleanCheckpoints", "true")
    .getOrCreate()
}
