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

import tbd.{Adjustable, Context, Input, ListConf, ListInput, Mutator}
import tbd.mod.{AdjustableList, Dest, Mod}
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

class ListSplitTest(input: ListInput[String, Int])
    extends Adjustable[(AdjustableList[String, Int], AdjustableList[String, Int])] {

  def run(implicit c: Context) = {
    val modList = input.getAdjustableList()
    modList.split(_._2 % 2 == 0)
  }
}

class ListSortTest(input: ListInput[String, Int])
    extends Adjustable[AdjustableList[String, Int]] {

  def run(implicit c: Context) = {
    val modList = input.getAdjustableList()
    modList.sort((a, b) => a._2 < b._2)
  }
}

class ChunkListMapTest(input: ListInput[Int, Int])
    extends Adjustable[AdjustableList[Int, Int]] {

  def run(implicit c: Context) = {
    val list = input.getAdjustableList()
    list.map((pair: (Int, Int)) => (pair._1, pair._2 - 2))
  }
}

class ListFilterTest(input: ListInput[Int, Int])
    extends Adjustable[AdjustableList[Int, Int]] {

  def run(implicit c: Context) = {
    val list = input.getAdjustableList()
    list.filter((pair: (Int, Int)) => pair._2 % 2 == 0)
  }
}

class ListReduceSumTest(input: ListInput[String, Int])
    extends Adjustable[Mod[(String, Int)]] {

  def run(implicit c: Context) = {
    val modList = input.getAdjustableList()
    val zero = mod { write(("", 0)) }
    modList.reduce(zero,
      (pair1: (String, Int), pair2: (String, Int)) => {
        (pair2._1, pair1._2 + pair2._2)
      })
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
    output.toBuffer().sortWith(_ < _) should be (Buffer(2, 4))

    input.update("one", 5)
    mutator.propagate()
    // (5 * 2), (2 * 2)
    output.toBuffer().sortWith(_ < _) should be (Buffer(4, 10))

    input.put("three", 4)
    mutator.propagate()
    // (5 * 2), (2 * 2), (4 * 2)
    output.toBuffer().sortWith(_ < _) should be (Buffer(4, 8, 10))

    input.update("three", 3)
    input.update("one", -2)
    input.put("four", 6)
    mutator.propagate()
    // (-2 * 2), (2 * 2), (3 * 2), (6 * 2)
    output.toBuffer().sortWith(_ < _) should be (Buffer(-4, 4, 6, 12))

    input.put("five", 9)
    input.update("five", 8)
    input.put("six", 10)
    input.update("four", 3)
    input.put("seven", 5)
    mutator.propagate()
    // (-2 * 2), (2 * 2), (3 * 2), (3 * 2), (8 * 2), (10 * 2), (5 * 2)
    output.toBuffer().sortWith(_ < _) should be
                                      (Buffer(-4, 4, 6, 6, 10, 16, 20))

    mutator.shutdown()
  }

  val maxKey = 1000
  val rand = new scala.util.Random()
  def addValue(input: Input[Int, Int], table: Map[Int, Int]) {
    var key = rand.nextInt(maxKey)
    val value = rand.nextInt(Int.MaxValue)
    while (table.contains(key)) {
      key = rand.nextInt(maxKey)
    }
    input.put(key, value)
    table += (key -> value)
  }

  def removeValue(input: Input[Int, Int], table: Map[Int, Int]) {
    if (table.size > 1) {
      var key = rand.nextInt(maxKey)
      while (!table.contains(key)) {
        key = rand.nextInt(maxKey)
      }
      input.remove(key)
      table -= key
    }
  }

  def updateValue(input: Input[Int, Int], table: Map[Int, Int]) {
    var key = rand.nextInt(maxKey)
    val value = rand.nextInt(Int.MaxValue)
    while (!table.contains(key)) {
      key = rand.nextInt(maxKey)
    }
    input.update(key, value)
    table(key) = value
  }

  def update(input: Input[Int, Int], table: Map[Int, Int]) {
    rand.nextInt(3) match {
      case 0 => addValue(input, table)
      case 1 => removeValue(input, table)
      case 2 => updateValue(input, table)
    }
  }

  "ChunkListMapTest" should "return the mapped list" in {
    for (partitions <- List(1, 2, 8)) {
      val mutator = new Mutator()
      val conf = new ListConf(partitions = partitions, chunkSize = 2)
      val input = mutator.createList[Int, Int](conf)
      val table = Map[Int, Int]()

      for (i <- 0 to 100) {
        addValue(input, table)
      }

      val test = new ChunkListMapTest(input)
      val output = mutator.run(test)

      var answer = table.values.map(_ - 2).toBuffer.sortWith(_ < _)
      output.toBuffer().sortWith(_ < _) should be (answer)

      for (i <- 0 to 5) {
        for (j <- 0 to 10) {
          update(input, table)
        }

        mutator.propagate()

        answer = table.values.map(_ - 2).toBuffer.sortWith(_ < _)
        output.toBuffer().sortWith(_ < _) should be (answer)
      }

      mutator.shutdown()
    }
  }

  "ListFilterTest" should "return the filtered list" in {
    for (partitions <- List(1, 2, 8)) {
      val mutator = new Mutator()
      val conf = new ListConf(partitions = partitions)
      val input = mutator.createList[Int, Int](conf)
      val table = Map[Int, Int]()

      for (i <- 0 to 100) {
        addValue(input, table)
      }

      val output = mutator.run(new ListFilterTest(input))
      var answer = table.values.filter(_ % 2 == 0).toBuffer.sortWith(_ < _)
      output.toBuffer().sortWith(_ < _) should be (answer)

      for (i <- 0 to 5) {
        for (j <- 0 to 10) {
          update(input, table)
        }

        mutator.propagate()

        answer = table.values.filter(_ % 2 == 0).toBuffer.sortWith(_ < _)
        output.toBuffer().sortWith(_ < _) should be (answer)
      }

      mutator.shutdown()
    }
  }

  "ListReduceSumTest" should "return the reduced list" in {
    val mutator = new Mutator()
    val input = mutator.createList[String, Int]()
    input.put("one", 1)
    input.put("two", 2)
    val output = mutator.run(new ListReduceSumTest(input))
    // 1 + 2 = 3
    output.read()._2 should be (3)

    input.put("three", 3)
    mutator.propagate()
    // 1 + 2 + 3 = 6
    output.read()._2 should be (6)

    input.update("one", 4)
    mutator.propagate()
    // 4 + 2 + 3 = 9
    output.read()._2 should be (9)

    input.update("three", 2)
    input.update("one", 7)
    mutator.propagate()
    // 7 + 2 + 2 = 11
    output.read()._2 should be (11)

    input.put("four", -1)
    input.put("five", 10)
    mutator.propagate()
    // 7 + 2 + 2 - 1 + 10 = 20
    output.read()._2 should be (20)

    input.put("six", -3)
    input.update("four", 3)
    input.update("three", 5)
    mutator.propagate()
    // 7 + 2 + 5 + 3 + 10 - 3 = 24
    output.read()._2 should be (24)

    mutator.shutdown()
  }

  it should "return the reduced big list" in {
    val mutator = new Mutator()
    val input = mutator.createList[String, Int]()
    var sum = 0

    for(i <- 0 to 100) {
      val r = rand.nextInt(100)
      input.put(i.toString, r)
      sum = sum + r
    }

    val output = mutator.run(new ListReduceSumTest(input))
    output.read()._2 should be (sum)

    mutator.shutdown()
  }

  "ListSplitTest" should "return the list, split in two" in {
    val mutator = new Mutator()
    val input = mutator.createList[String, Int](ListConf(partitions = 1))
    input.put("one", 0)
    input.put("two", 2)
    val output = mutator.run(new ListSplitTest(input))

    output._1.toBuffer().sortWith(_ < _) should be (Buffer(0, 2))
    output._2.toBuffer().sortWith(_ < _) should be (Buffer())

    input.put("three", 1)
    mutator.propagate()

    output._1.toBuffer().sortWith(_ < _) should be (Buffer(0, 2))
    output._2.toBuffer().sortWith(_ < _) should be (Buffer(1))

    input.update("two", 3)
    input.put("four", 4)
    mutator.propagate()

    output._1.toBuffer().sortWith(_ < _) should be (Buffer(0, 4))
    output._2.toBuffer().sortWith(_ < _) should be (Buffer(1, 3))

    input.put("seven", -1)
    mutator.propagate()

    output._1.toBuffer().sortWith(_ < _) should be (Buffer(0, 4))
    output._2.toBuffer().sortWith(_ < _) should be (Buffer(-1, 1, 3))

    input.update("two", 5)
    mutator.propagate()

    output._1.toBuffer().sortWith(_ < _) should be (Buffer(0, 4))
    output._2.toBuffer().sortWith(_ < _) should be (Buffer(-1, 1, 5))


    input.update("four", 7)
    mutator.propagate()

    output._1.toBuffer().sortWith(_ < _) should be (Buffer(0))
    output._2.toBuffer().sortWith(_ < _) should be (Buffer(-1, 1, 5, 7))


    input.update("two", 4)
    input.update("four", 2)
    input.update("seven", 8)
    mutator.propagate()

    output._1.toBuffer().sortWith(_ < _) should be (Buffer(0, 2, 4, 8))
    output._2.toBuffer().sortWith(_ < _) should be (Buffer(1))


    input.remove("four")
    input.remove("three")
    mutator.propagate()

    output._1.toBuffer().sortWith(_ < _) should be (Buffer(0, 4, 8))
    output._2.toBuffer().sortWith(_ < _) should be (Buffer())


    mutator.shutdown()
  }

  it should "return the big list, split in two" in {
    val mutator = new Mutator()
    val input = mutator.createList[String, Int](ListConf(partitions = 1))

    var data = new ArrayBuffer[Int]()

    for(i <- 0 to 1000) {
      val r = rand.nextInt(1000)
      input.put(i.toString, r)
      data += r
    }

    val output = mutator.run(new ListSplitTest(input))

    var answer = (data.filter(x => x % 2 == 0), data.filter(x => x % 2 != 0))

    output._1.toBuffer().sortWith(_ < _) should be (answer._1.sortWith(_ < _))
    output._2.toBuffer().sortWith(_ < _) should be (answer._2.sortWith(_ < _))

    for(i <- 0 to 500) {
      if(rand.nextInt(10) > 8) {
        val r = rand.nextInt(1000)
        input.update(i.toString, r)
        data(i) = r
      }
    }

    mutator.propagate()

    answer = (data.filter(x => x % 2 == 0), data.filter(x => x % 2 != 0))

    output._1.toBuffer().sortWith(_ < _) should be (answer._1.sortWith(_ < _))
    output._2.toBuffer().sortWith(_ < _) should be (answer._2.sortWith(_ < _))

    for(i <- 0 to 100) {
      if(i < data.size && rand.nextInt(10) > 8) {
        input.remove(i.toString)
        data(i) = -1
      }
    }

    data = data.filter(x => x != -1)

    mutator.propagate()

    answer = (data.filter(x => x % 2 == 0), data.filter(x => x % 2 != 0))

    output._1.toBuffer().sortWith(_ < _) should be (answer._1.sortWith(_ < _))
    output._2.toBuffer().sortWith(_ < _) should be (answer._2.sortWith(_ < _))

    mutator.shutdown()
  }

  "ListSortTest" should "return the sorted list" in {
    val mutator = new Mutator()
    val input = mutator.createList[String, Int](ListConf(partitions = 1))
    input.put("one", 0)
    input.put("two", 3)
    input.put("three", 2)
    input.put("four", 4)
    input.put("five", 1)
    val output = mutator.run(new ListSortTest(input))

    output.toBuffer() should be (Buffer(0, 1, 2, 3, 4))

    input.put("six", 5)
    mutator.propagate()
    output.toBuffer() should be (Buffer(0, 1, 2, 3, 4, 5))

    input.put("seven", -1)
    mutator.propagate()
    output.toBuffer() should be (Buffer(-1, 0, 1, 2, 3, 4, 5))

    input.update("two", 5)
    mutator.propagate()
    output.toBuffer() should be (Buffer(-1, 0, 1, 2, 4, 5, 5))

    mutator.shutdown()
  }

   it should "return the sorted big list" in {
    val mutator = new Mutator()
    val input = mutator.createList[String, Int](ListConf(partitions = 1))

    var data = new ArrayBuffer[Int]()

    for(i <- 0 to 100) {
      val r = rand.nextInt(1000)
      input.put(i.toString, r)
      data += r
    }

    val output = mutator.run(new ListSortTest(input))

    output.toBuffer() should be (data.sortWith(_ < _))

    for(i <- 0 to 100) {
      if(rand.nextInt(10) > 8) {
        val r = rand.nextInt(1000)
        input.update(i.toString, r)
        data(i) = r
      }
    }

    mutator.propagate()

    output.toBuffer() should be (data.sortWith(_ < _))

    for(i <- 0 to 100) {
      if(i < data.size && rand.nextInt(10) > 8) {
        input.remove(i.toString)
        data(i) = -1
      }
    }

    data = data.filter(x => x != -1)

    mutator.propagate()

    output.toBuffer() should be (data.sortWith(_ < _))

    mutator.shutdown()
  }
}
