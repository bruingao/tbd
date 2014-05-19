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

import tbd.{Adjustable, Mutator, TBD}
import tbd.mod.{AdjustableList, Mod}

class WCAdjust(partitions: Int, parallel: Boolean) extends Algorithm {
  var output: Mod[(Int, Map[String, Int])] = null

  def initialRun(mutator: Mutator) {
    output = mutator.run[Mod[(Int, Map[String, Int])]](this)
  }

  def checkOutput(chunks: Map[Int, String]): Boolean = {
    val answer = chunks.par.map(value => WC.wordcount(value._2)).reduce(WC.reduce)
    output.read()._2 == answer
  }

  def mapper(tbd: TBD, key: Int, s: String) = (key, WC.wordcount(s))

  def reducer(tbd: TBD, k1: Int, v1: Map[String, Int], k2: Int, v2: Map[String, Int]) =
    (k1, WC.reduce(v1, v2))

  def run(tbd: TBD): Mod[(Int, Map[String, Int])] = {
    val pages = tbd.input.getAdjustableList[Int, String](partitions)
    val counts = pages.map(tbd, mapper, parallel = parallel)
    val initialValue = tbd.createMod((0, Map[String, Int]()))
    counts.reduce(tbd, initialValue, reducer, parallel = parallel)
  }
}
