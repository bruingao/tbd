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

import scala.collection.mutable.{ArrayBuffer, Map}

abstract class Experiment(aConf: Map[String, _]) {
  val conf = aConf
  val algorithm = conf("algorithms")
  val count = conf("counts").asInstanceOf[String].toInt
  val chunkSize = conf("chunkSizes").asInstanceOf[String].toInt
  val mutations = conf("mutations").asInstanceOf[Array[String]]
  val partition = conf("partitions").asInstanceOf[String].toInt
  val percents = conf("percents").asInstanceOf[Array[String]]

  def run(): Map[String, Double]
}

object Experiment {
  val usage ="""Usage: run.sh [OPTION]...

Options:
  -a, --algorithms s,s,...   Algorithms to run, where s could be: map,nmap,
                               pmap,mpmap,mmap,filter,etc.
  -c, --check                Turns output checking on, for debugging.
  -h, --help                 Display this message.
  -m, --mutations s,s,...    Mutations to perform on the input data. Must be
                               one of 'update', 'insert', or 'remove'.
  -n, --counts n,n,...       Number of chunks to load initially.
  -o, --output chart,line,x  How to format the printed results - each of
                               'chart', 'line', and 'x' must be one of
                               'algorithms', 'chunkSizes', 'counts',
                               'partitons', or 'percents', with one required
                               to be 'percents'.
  -p, --partitions n,n,...   Number of partitions for the input data.
  -r, --repeat n             Number of times to repeat each experiment.
  -s, --chunkSizes n,n,...   Size of each chunk in the list, in KB.
  -%, --percents f,f,...     Percent of chunks to update before running
                               change propagation, as a decimal.=
  """

  var repeat = 3

  var inputSize = 0

  var check = false

  val confs = Map(("algorithms" -> Array("nmap", "mpmap")),
                  ("counts" -> Array("1000")),
		  ("chunkSizes" -> Array("1")),
                  ("mutations" -> Array("insert", "update", "remove")),
                  ("partitions" -> Array("8")),
                  ("percents" -> Array("initial", ".01", ".05", ".1")),
                  ("output" -> Array("percents", "algorithms", "counts")))

  val allResults = Map[Experiment, Map[String, Double]]()

  def round(value: Double): Double = {
    BigDecimal(value).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
  }


  def loadPages(): ArrayBuffer[String] = {
    val chunks = ArrayBuffer[String]()
    val elems = scala.xml.XML.loadFile("wiki.xml")

    (elems \\ "elem").map(elem => {
      (elem \\ "value").map(value => {
	chunks += value.text
      })
    })

    chunks
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
    var i = 0
    while (i < args.size) {
      args(i) match {
        case "--algorithms" | "-a" =>
          confs("algorithms") = args(i + 1).split(",")
	  i += 1
	case "--check" | "-c" =>
	  check = true
        case "--counts" | "-n" =>
          confs("counts") = args(i + 1).split(",")
	  i += 1
        case "--help" | "-h" =>
          println(usage)
          sys.exit()
        case "--mutations" | "-m" =>
          confs("mutations") = args(i + 1).split(",")
	  i += 1
        case "--partitions" | "-p" =>
          confs("partitions") = args(i + 1).split(",")
	  i += 1
        case "--percents" | "-%" =>
          confs("percents") = "initial" +: args(i + 1).split(",")
	  i += 1
        case "--repeat" | "-r" =>
          repeat = args(i + 1).toInt
	  i += 1
        case "--output" | "-o" =>
          confs("output") = args(i + 1).split(",")
	  i += 1
          assert(confs("output").size == 3)
	case "--chunkSizes" | "-s" =>
	  confs("chunkSizes") = args(i + 1).split(",")
	  i += 1
        case _ =>
          println("Unknown option " + args(i * 2) + "\n" + usage)
          sys.exit()
      }

      i += 1
    }

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
	      println(algorithm + "\t" + conf("counts") + " chunks - " +
		      conf("chunkSizes") + " / chunk")
	      println(results)
	      if (i != 0) {
		Experiment.allResults += (experiment -> results)
	      }
            }
          }
        }
      }
    }

    printCharts(confs("output")(0), confs("output")(1), confs("output")(2))
  }
}
