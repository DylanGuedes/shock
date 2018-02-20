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

package shock.tasks.processing

import shock.engines.{SparkEngine}
import shock.{Pipeline}
import play.api.libs.json.{JsValue}
import org.apache.spark.sql.{DataFrame, Column}
import org.apache.spark.sql.functions.{col, udf, dayofmonth, countDistinct,
  stddev_pop, mean, avg, pow, sqrt}
import org.apache.spark.sql.expressions.{Window, WindowSpec}

object WhereQuery extends ProcessingStrategy {
  /* Process a SparkSQL `where` query in a pipeline state
   * the `query` argument in the opts will be used
   */
  def process(engine: SparkEngine, pipeline: Pipeline, opts: JsValue): Pipeline = {
    val query: String = (opts \ "query").get.as[String]
    pipeline.state.get.where(query)
    pipeline.state.get.show()
    pipeline
  }
}

object SelectQuery extends ProcessingStrategy {
  /* Process a SparkSQL `select` query in a pipeline state
   * the `query` argument in the opts will be used
   */
  def process(engine: SparkEngine, pipeline: Pipeline, opts: JsValue): Pipeline = {
    val query: String = (opts \ "query").get.as[String]
    pipeline.state = Some(pipeline.state.get.select(query))
    pipeline.state.get.show()
    pipeline
  }
}

object DropNulls extends ProcessingStrategy {
  /* Drop all rows with at least one null value in Pipeline state
   */
  def process(engine: SparkEngine, pipeline: Pipeline, opts: JsValue): Pipeline = {
    pipeline.state = Some(pipeline.state.get.na.drop())
    pipeline.state.get.show()
    pipeline
  }
}

object GetArrayItem extends ProcessingStrategy {
  /* Drop all rows with at least one null value in Pipeline state
   */
  def process(engine: SparkEngine, pipeline: Pipeline, opts: JsValue): Pipeline = {
    val colName: String = (opts \ "colName").get.as[String]
    val newColName: String = (opts \ "newColName").get.as[String]
    val arrayPosition: Int = (opts \ "arrayPosition").get.as[Int]

    pipeline.state = Some(pipeline.state.get.withColumn(colName, col(colName).getItem(arrayPosition)))
    pipeline.state.get.show()
    pipeline
  }
}

object SciPopulisAnomaly extends ProcessingStrategy {
  def process(engine: SparkEngine, pipeline: Pipeline, opts: JsValue): Pipeline = {
    print("State:")
    print("count => ", pipeline.state.get.count())
    val df: DataFrame = pipeline.state.get.select(col("resources.capabilities") as "capabilities")
    df.printSchema()
    df.show()
    print("count => ", df.count())
      
    val df1: DataFrame = df.select(col("capabilities.edge_monitoring") as "edge_monitoring")
    print("df1 => ")
    df1.printSchema()
    df1.show()
    print("count => ", df1.count())
    print("end df...")

    val edge_monitoring: DataFrame = df1
      .select(
        "edge_monitoring.ref_hour",
        "edge_monitoring.date",
        "edge_monitoring.origin",
        "edge_monitoring.destination",
        "edge_monitoring.avg_speed")
      .na
      .drop
    print("edge_monitoring => ")
    edge_monitoring.printSchema()
    edge_monitoring.show()
    print(edge_monitoring.count())

    val firstItemDf: DataFrame = edge_monitoring
        .withColumn("ref_hour", col("ref_hour").getItem(0))
        .withColumn("date", col("date").getItem(0))
        .withColumn("origin", col("origin").getItem(0))
        .withColumn("destination", col("destination").getItem(0))
        .withColumn("avg_speed", col("avg_speed").getItem(0))
    print("firstitemdf => ")
    firstItemDf.printSchema()
    firstItemDf.show()
    print(firstItemDf.count())

    val extra_fields: DataFrame = firstItemDf
      .withColumn("origin_lat", col("origin.lat"))
      .withColumn("origin_lon", col("origin.lon"))
      .withColumn("dest_lat", col("destination.lat"))
      .withColumn("dest_lon", col("destination.lon"))
    print("extra_fields =>")
    extra_fields.printSchema()
    extra_fields.show()
    print(extra_fields.count())

    val perfect_df: DataFrame = extra_fields
      .select(
        col("date").cast("timestamp").alias("date"),
        col("ref_hour").alias("ref_hour"),
        col("avg_speed").alias("avg_speed"),
        col("origin_lat").cast("double").alias("origin_lat"),
        col("origin_lon").cast("double").alias("origin_lon"),
        col("dest_lat").cast("double").alias("dest_lat"),
        col("dest_lon").cast("double").alias("dest_lon"))
      .withColumn("day", dayofmonth(col("date")))
    print(perfect_df.count())
    perfect_df.printSchema()
    perfect_df.show()

    val w: WindowSpec = Window
      .partitionBy("origin_lat", "origin_lon", "dest_lat", "dest_lon", "ref_hour")

    val data = perfect_df
      .withColumn("stddev", stddevPopW(col("avg_speed"), w))
      .withColumn("mean", mean("avg_speed").over(w))

    print("data =>")
    data.printSchema()
    data.show()

    val detectionFunction = udf((value: Double, cdev: Double, cmean: Double) => {
      (value > (cdev*3+cmean)) || (value < (cdev*3-cmean))
    })

    val detections = data
      .withColumn("isAnomaly", detectionFunction(col("avg_speed"), col("stddev"), col("mean")))

    print("detections =>")
    detections.printSchema()
    detections.show()

    detections.select("isAnomaly").groupBy("isAnomaly").count().show()

    pipeline.state.get.printSchema()
    pipeline
  }

  def anomalyDetection(avgSpeed: Double, stdDev: Double, popMean: Double): Boolean = {
    (avgSpeed > (stdDev*3 + popMean)) || (avgSpeed < (stdDev*3 - popMean))
  }

  def stddevPopW(col: Column, w: WindowSpec): Column = {
    sqrt(avg(col * col).over(w) - pow(avg(col).over(w), 2))
  }
}
