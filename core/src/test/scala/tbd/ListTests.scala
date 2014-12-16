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
package tbd.test

import scala.collection.mutable.{ArrayBuffer, Buffer, Map}
import org.scalatest._

import tbd.{Adjustable, Context, Mod, Mutator}
import tbd.datastore.IntData
import tbd.list._
import tbd.TBD._

class ListMapTest(
    f: ((String, Int)) => (String, Int),
    input: ListInput[String, Int]
  ) extends Adjustable[AdjustableList[String, Int]] {

  def run(implicit c: Context) = {
    val list = input.getAdjustableList()
    list.map(f)
  }
}

class ListReduceTest(input: ListInput[Int, Int])
    extends Adjustable[Mod[(Int, Int)]] {

  def run(implicit c: Context) = {
    val modList = input.getAdjustableList()
    modList.reduce((pair1: (Int, Int), pair2: (Int, Int)) => {
      (pair2._1, pair1._2 + pair2._2)
    })
  }
}

class ListSortTest
    (input: ListInput[Int, Int])
  extends Adjustable[AdjustableList[Int, Int]] {

  def run(implicit c: Context) = {
    val modList = input.getAdjustableList()
    val comparator = (pair1: (Int, Int), pair2: (Int, Int)) => {pair1._1 - pair2._1}
    modList.mergesort(comparator)
  }
}

class ListSplitTest(input: ListInput[Int, Int])
    extends Adjustable[(AdjustableList[Int, Int], AdjustableList[Int, Int])] {

  def run(implicit c: Context) = {
    val modList = input.getAdjustableList()
    modList.split(_._2 % 2 == 0)
  }
}

class ListTests extends FlatSpec with Matchers {
  "ListMapTest" should "return the mapped list" in {
    val mutator = new Mutator()
    val input = mutator.createList[String, Int]()
    input.put("one", 1)
    input.put("two", 2)
    val f = (pair: (String, Int)) => (pair._1, pair._2 * 2)
    val output = mutator.run(new ListMapTest(f, input))
    // (1 * 2), (2 * 2)
    output.toBuffer(mutator).map(_._2).sortWith(_ < _) should be (Buffer(2, 4))

    input.update("one", 5)
    mutator.propagate()
    // (5 * 2), (2 * 2)
    output.toBuffer(mutator).map(_._2).sortWith(_ < _) should be (Buffer(4, 10))

    input.put("three", 4)
    mutator.propagate()
    // (5 * 2), (2 * 2), (4 * 2)
    output.toBuffer(mutator).map(_._2).sortWith(_ < _) should be (Buffer(4, 8, 10))

    input.update("three", 3)
    input.update("one", -2)
    input.put("four", 6)
    mutator.propagate()
    // (-2 * 2), (2 * 2), (3 * 2), (6 * 2)
    output.toBuffer(mutator).map(_._2).sortWith(_ < _) should be (Buffer(-4, 4, 6, 12))

    input.put("five", 9)
    input.update("five", 8)
    input.put("six", 10)
    input.update("four", 3)
    input.put("seven", 5)
    mutator.propagate()
    // (-2 * 2), (2 * 2), (3 * 2), (3 * 2), (8 * 2), (10 * 2), (5 * 2)
    output.toBuffer(mutator).map(_._2).sortWith(_ < _) should be (Buffer(-4, 4, 6, 6, 10, 16, 20))

    mutator.shutdown()
  }

  "ListReduceTest" should "return the reduced list" in {
    val mutator = new Mutator()
    val input = mutator.createList[Int, Int]()
    input.put(1, 1)
    input.put(2, 2)
    val output = mutator.run(new ListReduceTest(input))
    // 1 + 2 = 3
    mutator.read(output)._2 should be (3)

    input.put(3, 3)
    mutator.propagate()
    // 1 + 2 + 3 = 6
    mutator.read(output)._2 should be (6)

    input.update(1, 4)
    mutator.propagate()
    // 4 + 2 + 3 = 9
    mutator.read(output)._2 should be (9)

    input.update(3, 2)
    input.update(1, 7)
    mutator.propagate()
    // 7 + 2 + 2 = 11
    mutator.read(output)._2 should be (11)

    input.put(4, -1)
    input.put(5, 10)
    mutator.propagate()
    // 7 + 2 + 2 - 1 + 10 = 20
    mutator.read(output)._2 should be (20)

    input.put(6, -3)
    input.update(4, 3)
    input.update(3, 5)
    mutator.propagate()
    // 7 + 2 + 5 + 3 + 10 - 3 = 24
    mutator.read(output)._2 should be (24)

    mutator.shutdown()
  }

  "ListSortTest" should "return the sorted list" in {
    val mutator = new Mutator()
    val input = mutator.createList[Int, Int](ListConf(partitions = 1))
    input.put(0, 0)
    input.put(3, 3)
    input.put(2, 2)
    input.put(4, 4)
    input.put(1, 1)
    val output = mutator.run(new ListSortTest(input))

    output.toBuffer(mutator).map(_._2) should be (Buffer(0, 1, 2, 3, 4))

    input.put(5, 5)
    mutator.propagate()
    output.toBuffer(mutator).map(_._2) should be (Buffer(0, 1, 2, 3, 4, 5))

    input.put(-1, -1)
    mutator.propagate()
    output.toBuffer(mutator).map(_._2) should be (Buffer(-1, 0, 1, 2, 3, 4, 5))

    input.remove(3)
    mutator.propagate()
    output.toBuffer(mutator).map(_._2) should be (Buffer(-1, 0, 1, 2, 4, 5))

    mutator.shutdown()
  }

  "ListSplitTest" should "return the list, split in two" in {
    val mutator = new Mutator()
    val input = mutator.createList[Int, Int](ListConf(partitions = 1))
    input.put(1, 0)
    input.put(2, 2)
    val output = mutator.run(new ListSplitTest(input))

    output._1.toBuffer(mutator).map(_._2).sortWith(_ < _) should be (Buffer(0, 2))
    output._2.toBuffer(mutator).map(_._2).sortWith(_ < _) should be (Buffer())

    input.put(3, 1)
    mutator.propagate()

    output._1.toBuffer(mutator).map(_._2).sortWith(_ < _) should be (Buffer(0, 2))
    output._2.toBuffer(mutator).map(_._2).sortWith(_ < _) should be (Buffer(1))

    input.update(2, 3)
    input.put(4, 4)
    mutator.propagate()

    output._1.toBuffer(mutator).map(_._2).sortWith(_ < _) should be (Buffer(0, 4))
    output._2.toBuffer(mutator).map(_._2).sortWith(_ < _) should be (Buffer(1, 3))

    input.put(7, -1)
    mutator.propagate()

    output._1.toBuffer(mutator).map(_._2).sortWith(_ < _) should be (Buffer(0, 4))
    output._2.toBuffer(mutator).map(_._2).sortWith(_ < _) should be (Buffer(-1, 1, 3))

    input.update(2, 5)
    mutator.propagate()

    output._1.toBuffer(mutator).map(_._2).sortWith(_ < _) should be (Buffer(0, 4))
    output._2.toBuffer(mutator).map(_._2).sortWith(_ < _) should be (Buffer(-1, 1, 5))


    input.update(4, 7)
    mutator.propagate()

    output._1.toBuffer(mutator).map(_._2).sortWith(_ < _) should be (Buffer(0))
    output._2.toBuffer(mutator).map(_._2).sortWith(_ < _) should be (Buffer(-1, 1, 5, 7))


    input.update(2, 4)
    input.update(4, 2)
    input.update(7, 8)
    mutator.propagate()

    output._1.toBuffer(mutator).map(_._2).sortWith(_ < _) should be (Buffer(0, 2, 4, 8))
    output._2.toBuffer(mutator).map(_._2).sortWith(_ < _) should be (Buffer(1))

    input.remove(4)
    input.remove(3)
    mutator.propagate()

    output._1.toBuffer(mutator).map(_._2).sortWith(_ < _) should be (Buffer(0, 4, 8))
    output._2.toBuffer(mutator).map(_._2).sortWith(_ < _) should be (Buffer())


    mutator.shutdown()
  }
}
