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

import tbd._
import tbd.datastore.StringData
import tbd.list._

object SplitAlgorithm {
  def predicate(pair: (Int, String)): Boolean = {
    pair._2.length % 2 == 0
  }

  type SplitResult = (AdjustableList[Int, String], AdjustableList[Int, String])
}

class SplitAdjust(list: AdjustableList[Int, String])
  extends Adjustable[SplitAlgorithm.SplitResult] {

  def run(implicit c: Context) = {
    list.split((pair: (Int, String)) => SplitAlgorithm.predicate(pair))
  }
}

class SplitAlgorithm(_conf: Map[String, _], _listConf: ListConf)
    extends Algorithm[String, SplitAlgorithm.SplitResult](_conf, _listConf) {

  val input = mutator.createList[Int, String](listConf)

  val data = new StringData(input, count, mutations, Experiment.check)

  val adjust = new SplitAdjust(input.getAdjustableList())

  var naiveTable: GenIterable[String] = _
  def generateNaive() {
    data.generate()
    naiveTable = Vector(data.table.values.toSeq: _*).par
  }

  def runNaive() {
    naiveHelper(naiveTable)
  }

  private def naiveHelper(input: GenIterable[String]) = {
    input.partition(value => {
      SplitAlgorithm.predicate((0, value))
    })
  }

  def checkOutput(input: Map[Int, String], output: SplitAlgorithm.SplitResult): Boolean = {
    val sortedOutputA = output._1.toBuffer(mutator).map(_._2).sortWith(_ < _)
    val sortedOutputB = output._2.toBuffer(mutator).map(_._2).sortWith(_ < _)

    val answer = naiveHelper(input.values)

    sortedOutputA == answer._1.toBuffer.sortWith(_ < _)
    sortedOutputB == answer._2.toBuffer.sortWith(_ < _)
  }
}
