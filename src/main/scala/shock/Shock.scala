package ime.usp.br.shock

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Shock {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Shock")
    conf.setMaster("local")

    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(Seq(1,2,3,4))
    val rdd2 = rdd.map(_*2)
    println("before..")
    rdd2.collect.foreach(println)
    println("count: ", rdd2.count)
    println("end..")
  }
}
