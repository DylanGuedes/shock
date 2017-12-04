package shock.main

import shock.engines.spark._

object Shock {
  def main(args: Array[String]) {
    val engine = new SparkEngine(new MongoIngestionStrategy)
    engine.setup()
    engine.compute()

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
    println("before..")
    // rdd2.collect.foreach(println)
    // println("count: ", rdd2.count)
    println("end..")
  }
}
