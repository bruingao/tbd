/**
 * Copyright (C) 2013 Carnegie Mellon University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tbd.examples.list

import scala.collection.mutable.Map

abstract class Experiment(aConf: Map[String, _]) {
  val conf = aConf
  val algorithm = conf("algorithms")
  val chunkSize = conf("chunkSizes").asInstanceOf[String].toInt
  val count = conf("counts").asInstanceOf[String].toInt
  val mutations = conf("mutations").asInstanceOf[Array[String]]
  val partition = conf("partitions").asInstanceOf[String].toInt
  val percents = conf("percents").asInstanceOf[Array[String]]

  def run(): Map[String, Double]
}

object Experiment {
  val usage = """
    Usage: run.sh [--repeat num] [--counts int,int,...] [--percents float,float,...]
      [--chunkSize int] [--descriptions seq,par,memo,...] [--partitions int]
  """

  var repeat = 3

  val confs = Map(("algorithms" -> Array("nmap", "mpmap")),
                  ("chunkSizes" -> Array("20000")),
                  ("counts" -> Array("1000")),
                  ("mutations" -> Array("insert", "update", "remove")),
                  ("partitions" -> Array("10")),
                  ("percents" -> Array("initial", ".01", ".05", ".1")),
                  ("print" -> Array("percents", "algorithms", "counts")))

  val allResults = Map[Experiment, Map[String, Double]]()

  def round(value: Double): Double = {
    BigDecimal(value).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
  }

  /**
   * Prints out results formatted for copy-paste into spreadsheets. The output
   * will be divided into blocks differing along the 'charts' dimension, with
   * each block varying 'lines' along one dimension and 'x' along another.
   *
   * Either 'charts', 'lines', or 'x' must be 'percents', since the results
   * don't make any sense unless you vary initial vs. propagation. They must
   * also be three distinct values.
   */
  def printCharts(charts: String, lines: String, x: String) {
    for (chart <- confs(charts)) {
      print(chart + "\t")
      for (line <- confs(lines)) {
        print(line + "\t")
      }
      print("\n")

      for (xValue <- confs(x)) {
        print(xValue)

        for (line <- confs(lines)) {
          var total = 0.0
          var repeat = 0

          for ((experiment, results) <- allResults) {
            if (charts == "percents") {
              if (experiment.conf(lines) == line &&
                  experiment.conf(x) == xValue) {
                total += results(chart)
                repeat += 1
              }
            } else if (lines == "percents") {
              if (experiment.conf(x) == xValue &&
                  experiment.conf(charts) == chart) {
                total += results(line)
                repeat += 1
              }
            } else if (x == "percents") {
              if (experiment.conf(charts) == chart &&
                  experiment.conf(lines) == line) {
                total += results(xValue)
                repeat += 1
              }
            } else {
              println("Warning: print must vary 'percents'")
            }
          }

          print("\t" + round(total / repeat))
        }
        print("\n") 
      }
    }
  }

  def main(args: Array[String]) {
    def parse(list: List[String]) {
      list match {
        case Nil =>
        case "--algorithms" :: value :: tail =>
          confs("algorithms") = value.split(",")
          parse(tail)
        case "--chunkSizes" :: value :: tail =>
          confs("chunkSizes") = value.split(",")
          parse(tail)
        case "--counts" :: value :: tail =>
          confs("counts") = value.split(",")
          parse(tail)
        case "--mutations" :: value :: tail =>
          confs("mutations") = value.split(",")
          parse(tail)
        case "--partitions" :: value :: tail =>
          confs("partitions") = value.split(",")
          parse(tail)
        case "--percents" :: value :: tail =>
          confs("percents") = "initial" +: value.split(",")
          parse(tail)
        case "--repeat" :: value :: tail =>
          repeat = value.toInt
          parse(tail)
        case "--print" :: value :: tail =>
          confs("print") = value.split(",")
          assert(confs("print").size == 3)
          parse(tail)
        case option :: tail => println("Unknown option " + option + "\n" + usage)
      }
    }
    parse(args.toList)

    for (i <- 0 to repeat) {
      if (i == 0) {
        println("warmup")
      } else if (i == 1) {
        println("done warming up")
      }

      for (algorithm <- confs("algorithms")) {
        for (chunkSize <- confs("chunkSizes")) {
          for (count <- confs("counts")) {
            for (partition <- confs("partitions")) {
              val conf = Map(("algorithms" -> algorithm),
                             ("chunkSizes" -> chunkSize),
                             ("counts" -> count),
                             ("mutations" -> confs("mutations")),
                             ("partitions" -> partition),
                             ("percents" -> confs("percents")))
	      val experiment =
		if (algorithm.startsWith("n")) {
		  new ControlExperiment(conf)
		} else {
                  new AdjustableExperiment(conf)
		}

              val results = experiment.run()
	      println(algorithm + "\t" + count + "\t" + results)
	      if (i != 0) {
		Experiment.allResults += (experiment -> results)
              }
            }
          }
        }
      }
    }

    printCharts(confs("print")(0), confs("print")(1), confs("print")(2))
  }
}
