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

import tbd.Mutator
import tbd.debug._
import tbd.list._
import tbd.debug.TBD._

/*
 * Trait for test algorithms to run with the visualizer.
 */
trait TestAlgorithm[TbdOutputType, NativeOutputType]
    extends Adjustable[TbdOutputType] {
  //Reads the output and returns the result
  def getResult(output: TbdOutputType, mutator: Mutator): NativeOutputType
  //Processes the input in a conventional way to generate verification output.
  def getExpectedResult(input: Map[Int, Int]): NativeOutputType
  //Returns the list conf for the algorithm, so we can enforce a
  //certain type of list.
  def getListConf() = { new ListConf(partitions = 1) }

  var input: ListInput[Int, Int] = null
}

/*
 * A reduce algorithm test.
 */
class ListReduceSumTest()
    extends TestAlgorithm[Mod[(Int, Int)], Int] {

  def run(implicit c: Context): Mod[(Int, Int)] = {
    val modList = input.getAdjustableList()
    modList.reduce(
      (pair1: (Int, Int), pair2: (Int, Int)) => {
        (pair2._1, pair1._2 + pair2._2)
      })
  }

  def getResult(output: Mod[(Int, Int)], mutator: Mutator): Int = {
    mutator.read(output)._2
  }

  def getExpectedResult(input: Map[Int, Int]): Int = {
    input.values.foldLeft(0)(_ + _)
  }
}

/*
 * A sort algorithm test.
 */
class ListSortTest()
    extends TestAlgorithm[AdjustableList[Int, Int], Seq[Int]] {
  def run(implicit c: Context): AdjustableList[Int, Int] = {
    val modList = input.getAdjustableList()
    modList.quicksort((pair1: (Int, Int), pair2:(Int, Int)) => pair1._1 - pair2._1)
  }

  def getResult
      (output:  AdjustableList[Int, Int],
       mutator: Mutator): Seq[Int] = {
    output.toBuffer(mutator).map(_._1)
  }

  def getExpectedResult(input: Map[Int, Int]): Seq[Int] = {
    input.keys.toBuffer.sortWith(_ < _)
  }
}

/*
 * A split algorithm test.
 */
class ListSplitTest()
    extends TestAlgorithm[
      (AdjustableList[Int, Int], AdjustableList[Int, Int]),
      (Seq[Int], Seq[Int])] {
  def run(implicit c: Context):
      (AdjustableList[Int, Int], AdjustableList[Int, Int]) = {
    val modList = input.getAdjustableList()
    modList.split((a) => a._2 % 2 == 0)
  }

  def getResult
      (output:  (AdjustableList[Int, Int], AdjustableList[Int, Int]),
       mutator: Mutator): (Seq[Int], Seq[Int]) = {
    (output._1.toBuffer(mutator).map(_._2),
     output._2.toBuffer(mutator).map(_._2))
  }

  def getExpectedResult(input: Map[Int, Int]): (Seq[Int], Seq[Int]) = {
    (input.values.toBuffer.filter(x => x % 2 == 0),
     input.values.toBuffer.filter(x => x % 2 != 0))
  }
}

class ListMapTest()
    extends TestAlgorithm[AdjustableList[Int, Int], Seq[Int]] {
  def run(implicit c: Context): AdjustableList[Int, Int] = {
    val modList = input.getAdjustableList()
    modList.map((a) => (a._1, a._2 * 2))
  }

  def getResult
      (output: AdjustableList[Int, Int],
       mutator: Mutator): Seq[Int] = {
    output.toBuffer(mutator).map(_._2).sortWith(_ < _)
  }

  def getExpectedResult(input: Map[Int, Int]): Seq[Int] = {
    input.values.map(x => x * 2).toBuffer.sortWith(_ < _)
  }
}

/*
 * A test which creats a tiny DDG to check dependencies. 
 */
class ModDepTest()
    extends TestAlgorithm[Mod[Int], Int] {
  def run(implicit c: Context): Mod[Int] = {
    val modList = input.getAdjustableList().asInstanceOf[tbd.list.ModList[Int, Int]]

    mod{
      val a = 10
      read(mod {
        val b = 5
        read(modList.head) {
          case next => {
            write(next.value._2 + a + b)
          }
        }
      }) {
        case res => write(res)
      }
    }
  }

  def getResult(output: Mod[Int], mutator: Mutator): Int = {
    mutator.read(output)
  }

  def getExpectedResult(input: Map[Int, Int]): Int = {
    input.head._2 + 15
  }
}

class WordCountTest()
    extends TestAlgorithm[Mod[(Int, Int)], Int] {
  def run(implicit c: Context): Mod[(Int, Int)] = {
    val modList = input.getAdjustableList()
    val mapped = modList.map((a) => (a._1, a._2 * 2))
    mapped.reduce((pair1, pair2) => (pair1._1, pair1._2 + pair2._2))
  }

  def getResult(output: Mod[(Int, Int)], mutator: Mutator): Int = {
    mutator.read(output)._2
  }

  def getExpectedResult(input: Map[Int, Int]): Int = {
    input.values.map(_ * 2).reduce(_ + _)
  }
}
