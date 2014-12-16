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

import scala.collection.GenIterable
import scala.collection.mutable.Map

import tbd.{ Adjustable, Context }
import tbd.datastore.{ IntData, IntFileData }
import tbd.list._

class JoinAdjust(list1: AdjustableList[Int, Int],
  list2: AdjustableList[Int, Int])
  extends Adjustable[AdjustableList[Int, (Int, Int)]] {

  def condition (pair1: (Int, Int), pair2:(Int, Int)): Boolean = {
    pair1._1 == pair2._1
  }
  def run(implicit c: Context) = {
    list1.join(list2, condition)
  }
}

class JoinAlgorithm(_conf: Map[String, _], _listConf: ListConf)
  extends Algorithm[Int, AdjustableList[Int, (Int, Int)]](_conf, _listConf) {
  val input = mutator.createList[Int, Int](listConf)
  val data = new IntData(input, count, mutations, "data.txt")
  //val data = new IntFileData(input, "data.txt")

  val input2 = mutator.createList[Int, Int](listConf.copy(partitions = 1))
  val data2 = new IntData(input2, count, mutations, "data2.txt")
  //val data2 = new IntFileData(input2, "data2.txt")

  val adjust = new JoinAdjust(input.getAdjustableList(), input2.getAdjustableList())

  def generateNaive() = {
    data.generate()
    data2.generate()
    data2.load()
  }

  def runNaive() {
    naiveHelper(data.table, data2.table)
  }

  private def naiveHelper(input: Map[Int, Int], input2: Map[Int, Int]) = {
    val output = Map[Int, (Int, Int)]()
    for ((key, value) <- input) {
      for ((key2, value2) <- input2) {
        if (key == key2) {
          output(key) = (value, value2)
        }
      }
    }

    output
  }

  def checkOutput(
    input: Map[Int, Int],
    output: AdjustableList[Int, (Int, Int)]): Boolean = {
    val sortedOutput = output.toBuffer(mutator).sortWith(_._1 < _._1)
    val answer = naiveHelper(input, data2.table)
    val sortedAnswer = answer.toBuffer.sortWith(_._1 < _._1)

    sortedAnswer == sortedOutput
  }
}

class ChunkJoinAdjust(list1: AdjustableList[Int, Int],
  list2: AdjustableList[Int, Int])
  extends Adjustable[AdjustableList[Int, (Int, Int)]] {

  def condition (pair1: (Int, Int), pair2:(Int, Int)): Boolean = {
    pair1._1 == pair2._1
  }

  def run(implicit c: Context) = {
    list1.join(list2, condition)
  }
}

class ChunkJoinAlgorithm(_conf: Map[String, _], _listConf: ListConf)
  extends Algorithm[Int, AdjustableList[Int, (Int, Int)]](_conf, _listConf) {
  val input = mutator.createList[Int, Int](listConf)
  val data = new IntData(input, count, mutations)

  val input2 = mutator.createList[Int, Int](listConf)
  val data2 = new IntData(input2, count)

  val adjust = new ChunkJoinAdjust(input.getAdjustableList(), input2.getAdjustableList())

  def generateNaive() = {
    data.generate()
    data2.generate()
    data2.load()
  }

  def runNaive() {
    naiveHelper(data.table, data2.table)
  }

  private def naiveHelper(input: Map[Int, Int], input2: Map[Int, Int]) = {
    val output = Map[Int, (Int, Int)]()
    for ((key, value) <- input) {
      for ((key2, value2) <- input2) {
        if (key == key2) {
          output(key) = (value, value2)
        }
      }
    }

    output
  }

  def checkOutput(
    input: Map[Int, Int],
    output: AdjustableList[Int, (Int, Int)]): Boolean = {
    val sortedOutput = output.toBuffer(mutator).map(_._2).sortWith(_._1 < _._1)
    val answer = naiveHelper(input, data2.table)
    val sortedAnswer = answer.values.toBuffer.sortWith(_._1 < _._1)

    //println(sortedAnswer)
    //println(sortedOutput)
    sortedAnswer == sortedOutput
  }
}

class SortJoinAdjust(list1: AdjustableList[Int, Int],
  list2: AdjustableList[Int, Int])
  extends Adjustable[AdjustableList[Int, (Int, Int)]] {

  def run(implicit c: Context) = {
    list1.sortJoin(list2)
  }
}

class SortJoinAlgorithm(_conf: Map[String, _], _listConf: ListConf)
  extends Algorithm[Int, AdjustableList[Int, (Int, Int)]](_conf, _listConf) {
  val input = mutator.createList[Int, Int](listConf)
  val data = new IntData(input, count, mutations, "data.txt")
  //val data = new IntFileData(input, "data.txt")

  val input2 = mutator.createList[Int, Int](listConf)
  val data2 = new IntData(input2, count, mutations, "data2.txt")

  //val data2 = new IntFileData(input2, "data2.txt")
  val adjust = new SortJoinAdjust(input.getAdjustableList(), input2.getAdjustableList())

  def generateNaive() = {
    data.generate()
    data2.generate()
    data2.load()
  }

  def runNaive() {
    naiveHelper(data.table, data2.table)
  }

  private def naiveHelper(input: Map[Int, Int], input2: Map[Int, Int]) = {
    val sorted1 = input.toSeq.sortWith(_._1 < _._1)
    val sorted2 = input2.toSeq.sortWith(_._1 < _._1)

    val output = Map[Int, (Int, Int)]()
    def merge(one: Seq[(Int, Int)], two: Seq[(Int, Int)]) {
      if (one.size > 0 && two.size > 0) {
        if (one.head._1 == two.head._1) {
          output(one.head._1) = (one.head._2, two.head._2)
          merge(one.tail, two.tail)
        } else if (one.head._1 < two.head._1) {
          merge(one.tail, two)
        } else {
          merge(one, two.tail)
        }
      }
    }

    merge(sorted1, sorted2)

    output
  }

  def checkOutput(
    input: Map[Int, Int],
    output: AdjustableList[Int, (Int, Int)]): Boolean = {
    val sortedOutput = output.toBuffer(mutator).sortWith(_._1 < _._1)
    val answer = naiveHelper(input, data2.table)
    val sortedAnswer = answer.toBuffer.sortWith(_._1 < _._1)

    sortedAnswer == sortedOutput
  }
}
