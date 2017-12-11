package shock.main
import org.apache.spark.sql.types._

import org.bson.Document
import shock.engines.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.Dataset

object Shock {
  def main(args: Array[String]) {
    val engine = new SparkEngine(new MongoIngestionStrategy)
    engine.setup()

    val df: Dataset[Row] = engine.ingest()
    // val schema = StructType(StructField("name", StringType))
    // val df = engine.spark.createDataFrame(rdd)
    // val df = rdd.toDF()
    // println("SCHEMA=>")
    println("count now=> ", df.count())
    // val df2 = df.filter("capability == 'open' OR capability == 'slots_monitoring'")
    println("now now=>", df.count())
    df.printSchema()
    val df3 = df.select("bike_slots").filter("bike_slots is not null")
    println(df3.show())
    val statistics = df3.describe().selectExpr("summary as key", "bike_slots as value")
    statistics.show()
    statistics.write.format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "interscity")
      .save()
    // println(statistics.toJSON())

    // printSchema()
    // describe().show()
    // df.groupBy("capability").count().show()
    // df.where($"capability" == 

  //   println("LINES:")
  //   val lines = engine.spark.readStream.format("socket").option("host", "localhost").option("port", 9999).load()
  //
  // // Generate running word count
  // // val wordCounts = words.groupBy("value").count()
  //   println("AFTERLINES: ", lines)
  //
  //   val query = lines.writeStream.format("console").start()
  //
  //   query.awaitTermination()

    // val words = lines.as[String].flatMap(_.split(" "))
    //
    // val wordCounts = words.groupBy("value").count()

    // val rdd = sc.parallelize(Seq(1,2,3,4))
    // val rdd2 = rdd.map(_*2)
    // rdd2.collect.foreach(println)
    // println("count: ", rdd2.count)
  }
}
