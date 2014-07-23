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

import tbd.{Adjustable, Context, ListConf}
import tbd.mod.AdjustableList

object FilterAlgorithm {
  def predicate(pair: (Int, Int)): Boolean = {
    pair._2 % 2 == 0
  }
}

class FilterAlgorithm(_conf: Map[String, _], _listConf: ListConf)
    extends Algorithm[Int, AdjustableList[Int, Int]](_conf, _listConf) {
  val input = mutator.createList[Int, Int](listConf)

  data = new IntData(input, count, mutations)

  def runNaive(list: GenIterable[Int]) = {
    list.filter(FilterAlgorithm.predicate(0, _))
  }

  def checkOutput(table: Map[Int, Int], output: AdjustableList[Int, Int]) = {
    val sortedOutput = output.toBuffer().sortWith(_ < _)
    val answer = runNaive(table.values)

    sortedOutput == answer.toBuffer.sortWith(_ < _)
  }

  def filterer(pair: (Int, Int)) =
    FilterAlgorithm.predicate(pair)

  def run(implicit c: Context): AdjustableList[Int, Int] = {
    val pages = input.getAdjustableList()
    pages.filter(filterer)
  }
}
