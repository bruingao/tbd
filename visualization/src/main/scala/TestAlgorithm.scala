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

package tbd.visualization

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Map
import tbd.{Adjustable, Context, ListConf, ListInput, Mutator}
import tbd.mod.{AdjustableList, Mod}
import tbd.TBD._

trait TestAlgorithm[T] extends Adjustable[T] {
  def checkOutput(output: T, table: Map[Int, Int]): Boolean
  def getListConf() = { new ListConf(partitions = 1) }

  var input: ListInput[Int, Int] = null
}

class ListReduceSumTest()
    extends TestAlgorithm[Mod[(Int, Int)]]  {
  def run(implicit c: Context): Mod[(Int, Int)] = {
    val modList = input.getAdjustableList()
    val zero = mod{ write((0, 0)) }
    modList.reduce(zero,
      (pair1: (Int, Int), pair2: (Int, Int)) => {
        (pair2._1, pair1._2 + pair2._2)
      })
  }

  def checkOutput(output: Mod[(Int, Int)], table: Map[Int, Int]): Boolean = {
    val ca: Int = table.values.foldLeft(0)(_ + _)
    val a: Int = output.read()._2

    if(a != ca ) {
      println("Output: " + a)
      println("Expected: " + ca)

      false
    }
    true
  }
}

class ListQuicksortTest()
    extends TestAlgorithm[AdjustableList[Int, Int]] {
  def run(implicit c: Context): AdjustableList[Int, Int] = {
    val modList = input.getAdjustableList()
    modList.sort((a, b) => a._2 < b._2)
  }

  def checkOutput(output: AdjustableList[Int, Int], table: Map[Int, Int]): Boolean = {
    val ca = table.values.toBuffer.sortWith(_ < _)
    val a = output.toBuffer()

    if(a != ca ) {
      println("Output: " + a)
      println("Expected: " + ca)

      false
    }
    true
  }
}

class ListSplitTest()
    extends TestAlgorithm[(AdjustableList[Int, Int], AdjustableList[Int, Int])] {
  def run(implicit c: Context): (AdjustableList[Int, Int], AdjustableList[Int, Int]) = {
    val modList = input.getAdjustableList()
    modList.split(_._2 % 2 == 0)
  }

 def checkOutput(output: (AdjustableList[Int, Int], AdjustableList[Int, Int]), table: Map[Int, Int]): Boolean = {
    val ca = table.values.toBuffer.filter(x => x % 2 == 0)
    val a = output._1.toBuffer()
    val cb = table.values.toBuffer.filter(x => x % 2 != 0)
    val b = output._2.toBuffer()

    if(a != ca || b != cb) {
      println("OutputA: " + a)
      println("ExpectedA: " + ca)
      println("OutputB: " + b)
      println("ExpectedB: " + cb)

      false
    }
    true
  }
}
