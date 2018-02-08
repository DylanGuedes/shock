package shock

import shock.KC

object Shock {
  var options: Map[String, String] = Map[String, String]()

  def main(args: Array[String]) {
    this.options = parseOptions(args)

    val consumer: KC = new KC(options)
    println("Start consuming...")
    consumer.consume()
  }

  def parseOptions(args: Array[String]): Map[String, String] = {
    val splittedArgs: List[Array[String]] = args.sliding(2, 2).toList
    splittedArgs.foldLeft(Map[String, String]())(
      (t: Map[String, String], opt: Array[String]) => t + (opt(0) -> opt(1))
    )
  }
}
