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

import scala.collection.{GenIterable, GenMap, Seq}
import scala.collection.mutable.Map

import tbd.{Adjustable, Context, ListConf, Mutator}
import tbd.mod.{AdjustableList, Mod}

object SplitAlgorithm {
  def predicate(pair: (Int, String)): Boolean = {
    pair._2.length % 2 == 0
  }

  type SplitResult = (AdjustableList[Int, String], AdjustableList[Int, String])
}

class SplitAlgorithm(_conf: Map[String, _], _listConf: ListConf)
    extends Algorithm[String, SplitAlgorithm.SplitResult](_conf, _listConf) {
  import SplitAlgorithm._

  val input = mutator.createList[Int, String](listConf)

  data = new WCData(input, count, mutations)

  def runNaive(input: GenIterable[String]) = {
    input.partition(value => {
      SplitAlgorithm.predicate((0, value))
    })
  }

  def checkOutput(input: Map[Int, String], output: SplitResult): Boolean = {
    val sortedOutputA = output._1.toBuffer.sortWith(_ < _)
    val sortedOutputB = output._2.toBuffer.sortWith(_ < _)

    val answer = runNaive(input.values)

    sortedOutputA == answer._1.toBuffer.sortWith(_ < _)
    sortedOutputB == answer._2.toBuffer.sortWith(_ < _)
  }

  def run(implicit c: Context): SplitResult = {
    val pages = input.getAdjustableList()

    pages.split((pair: (Int, String)) => SplitAlgorithm.predicate(pair))
  }
}
