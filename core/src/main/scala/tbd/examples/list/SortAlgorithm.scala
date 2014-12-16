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
import scala.collection.immutable.TreeSet
import scala.collection.mutable.Map

import tbd._
import tbd.datastore.{IntData, IntFileData}
import tbd.list._

class QuickSortAdjust(list: AdjustableList[Int, Int])
  extends Adjustable[AdjustableList[Int, Int]] {
  def run(implicit c: Context) = {
    list.quicksort ((pair1: (Int, Int), pair2:(Int, Int)) => pair1._1 - pair2._1)
  }
}

class QuickSortAlgorithm(_conf: Map[String, _], _listConf: ListConf)
    extends Algorithm[Int, AdjustableList[Int, Int]](_conf, _listConf) {
  val input = mutator.createList[Int, Int](listConf)

  val adjust = new QuickSortAdjust(input.getAdjustableList())

  val data =
    if (Experiment.file != "")
      new IntFileData(input, Experiment.file)
    else
      new IntData(input, count, mutations)

  def generateNaive() {
    data.generate()
  }

  def runNaive() {
    naiveHelper(data.table)
  }

  private def naiveHelper(input: Map[Int, Int]) = {
    input.toBuffer.sortWith(_._1 < _._1)
  }

  def checkOutput(
      input: Map[Int, Int],
      output: AdjustableList[Int, Int]) = {
    val sortedOutput = output.toBuffer(mutator)
    val answer = naiveHelper(input)

    //println(sortedOutput)
    //println(answer.toBuffer)

    sortedOutput == answer.toBuffer
  }
}

class MergeSortAdjust(list: AdjustableList[Int, Int])
  extends Adjustable[AdjustableList[Int, Int]] {
  def run(implicit c: Context) = {
    list.mergesort((pair1: (Int, Int), pair2:(Int, Int)) => pair1._1 - pair2._1)
  }
}

class MergeSortAlgorithm(_conf: Map[String, _], _listConf: ListConf)
    extends Algorithm[Int, AdjustableList[Int, Int]](_conf, _listConf) {
  val input = mutator.createList[Int, Int](listConf)

  val adjust = new MergeSortAdjust(input.getAdjustableList())

  val data =
    if (Experiment.file != "")
      new IntFileData(input, Experiment.file)
    else
      new IntData(input, count, mutations)

  def generateNaive() {
    data.generate()
  }

  def runNaive() {
    naiveHelper(data.table)
  }

  private def naiveHelper(input: Map[Int, Int]) = {
    input.toBuffer.sortWith(_._1 < _._1)
  }

  def checkOutput(
      input: Map[Int, Int],
      output: AdjustableList[Int, Int]) = {
    val sortedOutput = output.toBuffer(mutator)
    val answer = naiveHelper(input)

    //println(sortedOutput)
    //println(answer.toBuffer)

    sortedOutput == answer.toBuffer
  }
}
