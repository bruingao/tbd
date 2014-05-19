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

import java.io.{BufferedInputStream, File, FileInputStream}
import scala.collection.mutable.{ArrayBuffer, Map}

import tbd.Mutator

class AdjustableExperiment(aConf: Map[String, _])
    extends Experiment(aConf) {
  val chunks = ArrayBuffer[String]()

  val rand = new scala.util.Random()
  val maxKey = count * 10
  def addValue(mutator: Mutator, table: Map[Int, String]) {
    if (chunks.size == 0) {
      chunks ++= Experiment.loadPages()
    }

    var key = rand.nextInt(maxKey)
    val value = rand.nextInt(Int.MaxValue)
    while (table.contains(key)) {
      key = rand.nextInt(maxKey)
    }
    mutator.put(key, chunks.head)
    table += (key -> chunks.head)
    chunks -= chunks.head
  }

  def removeValue(mutator: Mutator, table: Map[Int, String]) {
    if (table.size > 1) {
      var key = rand.nextInt(maxKey)
      while (!table.contains(key)) {
        key = rand.nextInt(maxKey)
      }
      mutator.remove(key)
      table -= key
    } else {
      addValue(mutator, table)
    }
  }

  def updateValue(mutator: Mutator, table: Map[Int, String]) {
    if (chunks.size == 0) {
      chunks ++= Experiment.loadPages()
    }

    var key = rand.nextInt(maxKey)
    val value = rand.nextInt(Int.MaxValue)
    while (!table.contains(key)) {
      key = rand.nextInt(maxKey)
    }
    mutator.update(key, chunks.head)
    table(key) = chunks.head
    chunks -= chunks.head
  }

  def update(mutator: Mutator, table: Map[Int, String]) {
    mutations(rand.nextInt(mutations.size)) match {
      case "insert" => addValue(mutator, table)
      case "remove" => removeValue(mutator, table)
      case "update" => updateValue(mutator, table)
    }
  }

  def run(): Map[String, Double] = {
    val results = Map[String, Double]()

    val mutator = new Mutator()
    val table = Map[Int, String]()

    while (table.size < count) {
      addValue(mutator, table)
    }

    val alg = algorithm match {
      // Map
      case "map" => new MapAdjust(partition, false, false)
      case "pmap" => new MapAdjust(partition, true, false)
      case "mmap" => new MapAdjust(partition, false, true)
      case "mpmap" => new MapAdjust(partition, true, true)

      case "cmap" => new ChunkMapAdjust(partition, chunkSize, false, false)
      case "pcmap" => new ChunkMapAdjust(partition, chunkSize, true, false)
      case "mcmap" => new ChunkMapAdjust(partition, chunkSize, false, true)
      case "mpcmap" => new ChunkMapAdjust(partition, chunkSize, true, true)
      // Filter
      case "filter" => new FilterAdjust(partition, false, false)
      case "pfilter" => new FilterAdjust(partition, true, false)
      case "mfilter" => new FilterAdjust(partition, false, true)
      case "mpfilter" => new FilterAdjust(partition, true, true)
      // Wordcount
      case "wc" => new WCAdjust(partition, false)
      case "pwc" => new WCAdjust(partition, true)
    }

    val before = System.currentTimeMillis()
    alg.initialRun(mutator)
    val initialElapsed = System.currentTimeMillis() - before

    if (Experiment.check) {
      assert(alg.checkOutput(table))
    }

    results("initial") = initialElapsed

    for (percent <- percents) {
      if (percent != "initial") {
        var i =  0
        while (i < percent.toDouble * count) {
	  i += 1
	  update(mutator, table)
        }

        val before2 = System.currentTimeMillis()
        mutator.propagate()
        val elapsed = System.currentTimeMillis() - before2

	if (Experiment.check) {
          assert(alg.checkOutput(table))
	}

        results(percent) = elapsed
      }
    }

    mutator.shutdown()

    results
  }
}
