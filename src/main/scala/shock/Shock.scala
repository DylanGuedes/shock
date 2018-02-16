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

package shock

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
