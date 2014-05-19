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

import scala.collection.mutable.{Buffer, Map}
import org.scalatest._

import tbd.{Adjustable, Changeable, Mutator, TBD}
import tbd.mod.{AdjustableList, Dest, Mod}

class ListMapTest(
    f: (TBD, String, Int) => (String, Int),
    parallel: Boolean) extends Adjustable {
  def run(tbd: TBD): AdjustableList[String, Int] = {
    val list = tbd.input.getAdjustableList[String, Int](partitions = 1)
    list.map(tbd, f)
  }
}

class ListMemoMapTest extends Adjustable {
  def run(tbd: TBD): AdjustableList[String, Int] = {
    val list = tbd.input.getAdjustableList[String, Int](partitions = 1)
    list.map(tbd,
             (tbd: TBD, key: String, value: Int) => (key, value + 3),
             memoized = true)
  }
}

class ChunkListMapTest(partitions: Int) extends Adjustable {
  def run(tbd: TBD): AdjustableList[Int, Int] = {
    val list = tbd.input.getAdjustableList[Int, Int](partitions = partitions,
                                                     chunkSize = 2,
                                                     chunkSizer = _ => 1)
    list.map(tbd, (tbd: TBD, key: Int, value: Int) => (key, value - 2))
  }
}

class ListFilterTest(partitions: Int) extends Adjustable {
  def run(tbd: TBD): AdjustableList[Int, Int] = {
    val list = tbd.input.getAdjustableList[Int, Int](partitions = partitions)
    list.filter(tbd, (key: Int, value: Int) => value % 2 == 0)
  }
}

class ListReduceSumTest extends Adjustable {
  def run(tbd: TBD): Mod[(String, Int)] = {
    val modList = tbd.input.getAdjustableList[String, Int](partitions = 1)
    val zero = tbd.mod((dest : Dest[(String, Int)]) => tbd.write(dest, ("", 0)))
    modList.reduce(tbd, zero,
      (tbd: TBD, ka: String, a: Int, kb: String, b: Int) => {
        (kb, a + b)
      })
  }
}

class ListTests extends FlatSpec with Matchers {
  "ListMapTest" should "return the mapped list" in {
    val mutator = new Mutator()
    mutator.put("one", 1)
    mutator.put("two", 2)
    val f = (tbd: TBD, key: String, value: Int) => (key, value * 2)
    val output =
      mutator.run[AdjustableList[String, Int]](new ListMapTest(f, false))
    // (1 * 2), (2 * 2)
    output.toBuffer().sortWith(_ < _) should be (Buffer(2, 4))

    mutator.update("one", 5)
    mutator.propagate()
    // (5 * 2), (2 * 2)
    output.toBuffer().sortWith(_ < _) should be (Buffer(4, 10))

    mutator.put("three", 4)
    mutator.propagate()
    // (5 * 2), (2 * 2), (4 * 2)
    output.toBuffer().sortWith(_ < _) should be (Buffer(4, 8, 10))

    mutator.update("three", 3)
    mutator.update("one", -2)
    mutator.put("four", 6)
    mutator.propagate()
    // (-2 * 2), (2 * 2), (3 * 2), (6 * 2)
    output.toBuffer().sortWith(_ < _) should be (Buffer(-4, 4, 6, 12))

    mutator.put("five", 9)
    mutator.update("five", 8)
    mutator.put("six", 10)
    mutator.update("four", 3)
    mutator.put("seven", 5)
    mutator.propagate()
    // (-2 * 2), (2 * 2), (3 * 2), (3 * 2), (8 * 2), (10 * 2), (5 * 2)
    output.toBuffer().sortWith(_ < _) should be 
                                      (Buffer(-4, 4, 6, 6, 10, 16, 20))

    mutator.shutdown()
  }

  "ListParMapTest" should "return the mapped list" in {
    val mutator = new Mutator()
    mutator.put("one", 1)
    mutator.put("two", 2)
    val f = (tbd: TBD, key: String, value: Int) => (key, value + 1)
    val output = mutator.run[AdjustableList[String, Int]](new ListMapTest(f, true))
    // (1 + 1), (2 + 1)
    output.toBuffer().sortWith(_ < _) should be (Buffer(2, 3))

    mutator.put("three", 3)
    mutator.propagate()
    // (1 + 1), (2 + 1), (3 + 1)
    output.toBuffer().sortWith(_ < _) should be (Buffer(2, 3, 4))

    mutator.update("one", 4)
    mutator.propagate()
    // (4 + 1), (2 + 1), (3 + 1)
    output.toBuffer().sortWith(_ < _) should be (Buffer(3, 4, 5))

    mutator.update("three", 2)
    mutator.update("one", 7)
    mutator.propagate()
    // (7 + 1), (2 + 1), (2 + 1)
    output.toBuffer().sortWith(_ < _) should be (Buffer(3, 3, 8))

    mutator.put("four", -1)
    mutator.put("five", 10)
    mutator.propagate()
    // (7 + 1), (2 + 1), (2 + 1), (-1 + 1), (10 + 1)
    output.toBuffer().sortWith(_ < _) should be (Buffer(0, 3, 3, 8, 11))

    mutator.put("six", -3)
    mutator.update("four", 3)
    mutator.update("three", 5)
    mutator.propagate()
    // (7 + 1), (2 + 1), (5 + 1), (3 + 1), (10 + 1), (-3 + 1)
    output.toBuffer().sortWith(_ < _) should be (Buffer(-2, 3, 4, 6, 8, 11))

    mutator.shutdown()
  }

  "ListMemoMapTest" should "return the mapped AdjustableList" in {
    val mutator = new Mutator()
    mutator.put("one", 1)
    mutator.put("two", 2)
    mutator.put("three", 3)
    mutator.put("four", 4)
    val output = mutator.run[AdjustableList[String, Int]](new ListMemoMapTest())
    output.toBuffer().sortWith(_ < _) should be (Buffer(4, 5, 6, 7))

    mutator.remove("two")
    mutator.propagate()
    output.toBuffer().sortWith(_ < _) should be (Buffer(4, 6, 7))

    mutator.put("five", 5)
    mutator.remove("three")
    mutator.propagate()
    output.toBuffer().sortWith(_ < _) should be (Buffer(4, 7, 8))

    mutator.put("six", 6)
    mutator.put("seven", 7)
    mutator.put("eight", 8)
    mutator.remove("six")
    mutator.propagate()
    output.toBuffer().sortWith(_ < _) should be (Buffer(4, 7, 8, 10, 11))

    mutator.remove("one")
    mutator.remove("five")
    mutator.remove("eight")
    mutator.propagate()
    output.toBuffer().sortWith(_ < _) should be (Buffer(7, 10))

    mutator.update("four", -4)
    mutator.put("nine", 9)
    mutator.remove("seven")
    mutator.propagate()
    output.toBuffer().sortWith(_ < _) should be (Buffer(-1, 12))
  }

  val maxKey = 1000
  val rand = new scala.util.Random()
  def addValue(mutator: Mutator, table: Map[Int, Int]) {
    var key = rand.nextInt(maxKey)
    val value = rand.nextInt(Int.MaxValue)
    while (table.contains(key)) {
      key = rand.nextInt(maxKey)
    }
    mutator.put(key, value)
    table += (key -> value)
  }

  def removeValue(mutator: Mutator, table: Map[Int, Int]) {
    if (table.size > 1) {
      var key = rand.nextInt(maxKey)
      while (!table.contains(key)) {
        key = rand.nextInt(maxKey)
      }
      mutator.remove(key)
      table -= key
    }
  }

  def updateValue(mutator: Mutator, table: Map[Int, Int]) {
    var key = rand.nextInt(maxKey)
    val value = rand.nextInt(Int.MaxValue)
    while (!table.contains(key)) {
      key = rand.nextInt(maxKey)
    }
    mutator.update(key, value)
    table(key) = value
  }

  def update(mutator: Mutator, table: Map[Int, Int]) {
    rand.nextInt(3) match {
      case 0 => addValue(mutator, table)
      case 1 => removeValue(mutator, table)
      case 2 => updateValue(mutator, table)
    }
  }

  "ChunkListMapTest" should "return the filtered list" in {
    for (partitions <- List(1, 2, 8)) {
      val mutator = new Mutator()
      val table = Map[Int, Int]()

      for (i <- 0 to 100) {
        addValue(mutator, table)
      }

      val test = new ChunkListMapTest(partitions)
      val output = mutator.run[AdjustableList[String, Int]](test)

      var answer = table.values.map(_ - 2).toBuffer.sortWith(_ < _)
      output.toBuffer().sortWith(_ < _) should be (answer)

      for (i <- 0 to 5) {
        for (j <- 0 to 10) {
          update(mutator, table)
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
      val table = Map[Int, Int]()

      for (i <- 0 to 100) {
        addValue(mutator, table)
      }

      val output = mutator.run[AdjustableList[String, Int]](
                                            new ListFilterTest(partitions))
      var answer = table.values.filter(_ % 2 == 0).toBuffer.sortWith(_ < _)
      output.toBuffer().sortWith(_ < _) should be (answer)

      for (i <- 0 to 5) {
        for (j <- 0 to 10) {
          update(mutator, table)
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
    mutator.put("one", 1)
    mutator.put("two", 2)
    val output = mutator.run[Mod[(String, Int)]](new ListReduceSumTest())
    // 1 + 2 = 3
    output.read()._2 should be (3)

    mutator.put("three", 3)
    mutator.propagate()
    // 1 + 2 + 3 = 6
    output.read()._2 should be (6)

    mutator.update("one", 4)
    mutator.propagate()
    // 4 + 2 + 3 = 9
    output.read()._2 should be (9)

    mutator.update("three", 2)
    mutator.update("one", 7)
    mutator.propagate()
    // 7 + 2 + 2 = 11
    output.read()._2 should be (11)

    mutator.put("four", -1)
    mutator.put("five", 10)
    mutator.propagate()
    // 7 + 2 + 2 - 1 + 10 = 20
    output.read()._2 should be (20)

    mutator.put("six", -3)
    mutator.update("four", 3)
    mutator.update("three", 5)
    mutator.propagate()
    // 7 + 2 + 5 + 3 + 10 - 3 = 24
    output.read()._2 should be (24)

    mutator.shutdown()
  }

  "BigListReduceSumTest" should "return the reduced big list" in {    
    val mutator = new Mutator()
    var sum = 0

    for(i <- 0 to 100) {
      val r = rand.nextInt(100)
      mutator.put(i.toString, r)
      sum = sum + r
    }
    
    val output = mutator.run[Mod[(String, Int)]](new ListReduceSumTest())
    output.read()._2 should be (sum)

    mutator.shutdown()
  }
}
