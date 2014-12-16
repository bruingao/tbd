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

import org.scalatest._

import tbd._
import tbd.datastore.IntData
import tbd.list._

class ListTest(input: ListInput[Int, Int])
  extends Adjustable[AdjustableList[Int, Int]] {
  def run(implicit c: Context) = {
    input.getAdjustableList()
  }
}

class MutatorTests extends FlatSpec with Matchers {
  val intensity = 10
  def runTest(mutator: Mutator,
    adjustable: Adjustable[AdjustableList[Int, Int]],
    input: ListInput[Int, Int],
    sorted: Boolean) {

    def check(output: AdjustableList[Int, Int], data: IntData): Boolean = {
      val sortedAnswer = data.table.toBuffer.sortWith(_._1 < _._1)

      val sortedOutput =
        if (sorted)
          output.toBuffer(mutator)
        else
          output.toBuffer(mutator).sortWith(_._1 < _._1)

      sortedOutput == sortedAnswer
    }

    val data = new IntData(
      input, intensity, Array("insert", "remove", "update"))
    data.generate()
    data.load()

    val output = mutator.run(adjustable)
    check(output, data)

    for (j <- 0 to intensity) {
      data.updateValue()
      assert(check(output, data))
    }

    for (j <- 0 to intensity) {
      data.addValue()
      assert(check(output, data))
    }

    while (data.table.size > 0) {
      data.removeValue()
      assert(check(output, data))
    }

    for (j <- 0 to intensity) {
      data.update(1)
      assert(check(output, data))
    }
  }

  "AdjustableListTests" should "update the AdjustableList correctly" in {
    for (partitions <- 1 to 2) {
      for (chunkSize <- 1 to 2) {
        val mutator = new Mutator()

        val conf = new ListConf(partitions = partitions, chunkSize = chunkSize)
        val input = mutator.createList[Int, Int](conf)
        runTest(mutator, new ListTest(input), input, false)

        mutator.shutdown()
      }
    }
  }

  /*"SortedListTests" should "update the sorted AdjustableList correctly" in {
    val mutator = new Mutator()
    val conf = new ListConf(partitions = 1, chunkSize = 1, sorted = true)
    val input = mutator.createList[Int, Int](conf)
    runTest(mutator, new ListTest(input), input, true)

    mutator.shutdown()
  }*/
}
