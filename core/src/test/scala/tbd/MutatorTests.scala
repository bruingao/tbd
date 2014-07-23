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
import scala.collection.mutable.Map

import tbd._
import tbd.mod.AdjustableList

class ListTest(input: ListInput[Int, Int])
    extends Adjustable[AdjustableList[Int, Int]] {
  def run(implicit c: Context) = {
    input.getAdjustableList()
  }
}

class ChunkListTest(input: ChunkListInput[Int, Int])
    extends Adjustable[AdjustableList[Int, Int]] {
  def run(implicit c: Context) = {
    input.getChunkList()
  }
}

class MutatorTests extends FlatSpec with Matchers {
  val rand = new scala.util.Random()

  def updateValue(input: Input[Int, Int], answer: Map[Int, Int]) {
    val index = rand.nextInt(answer.size)
    var i = 0
    for ((key, value) <- answer) {
      if (i == index) {
	val newValue = rand.nextInt(1000)
	input.update(key, newValue)
	answer(key) = newValue
      }

      i += 1
    }
  }

  def insertValue(input: Input[Int, Int], answer: Map[Int, Int], i: Int) {
    val value = rand.nextInt(1000)
    answer += (i -> value)
    input.put(i, value)
  }

  def removeValue(input: Input[Int, Int], answer: Map[Int, Int]) {
    val index = rand.nextInt(answer.size)
    var j = 0
    for ((key, value) <- answer) {
      if (j == index) {
	input.remove(key)
	answer -= key
      }
      j += 1
    }
  }

  def runTest(
      mutator: Mutator,
      adjustable: Adjustable[AdjustableList[Int, Int]],
      input: Input[Int, Int]) {
    val answer = Map[Int, Int]()

    var  i  = 0
    while (i < 100) {
      answer += (i -> i)
      input.put(i, i)
      i += 1
    }

    val output = mutator.run(adjustable)
    var sortedAnswer = answer.values.toBuffer.sortWith(_ < _)
    output.toBuffer().sortWith(_ < _) should be (sortedAnswer)

    for (j <- 0 to i - 1) {
      updateValue(input, answer)
      sortedAnswer = answer.values.toBuffer.sortWith(_ < _)
      output.toBuffer().sortWith(_ < _) should be (sortedAnswer)
    }

    while (i < 200) {
      insertValue(input, answer, i)
      i += 1
      sortedAnswer = answer.values.toBuffer.sortWith(_ < _)
      output.toBuffer().sortWith(_ < _) should be (sortedAnswer)
    }

    for (j <- 0 to 99) {
      removeValue(input, answer)
      sortedAnswer = answer.values.toBuffer.sortWith(_ < _)
      output.toBuffer().sortWith(_ < _) should be (sortedAnswer)
    }

    for (j <- 0 to 99) {
      rand.nextInt(3) match {
	case 0 => {
	  insertValue(input, answer, i)
	  i += 1
	}
	case 1 => {
	  removeValue(input, answer)
	}
	case 2 => {
	  updateValue(input, answer)
	}
      }
      sortedAnswer = answer.values.toBuffer.sortWith(_ < _)
      output.toBuffer().sortWith(_ < _) should be (sortedAnswer)
    }
  }

  "AdjustableListTests" should "update the AdjustableList correctly" in {
    for (partitions <- 1 to 2) {
      for (chunkSize <- 1 to 2) {
	val mutator = new Mutator()

	val conf = new ListConf(partitions = partitions, chunkSize = chunkSize)
	if (chunkSize == 1) {
	  val input = mutator.createList[Int, Int](conf)
	  runTest(mutator, new ListTest(input), input)
	} else {
	  val input = mutator.createChunkList[Int, Int](conf)
	  runTest(mutator, new ChunkListTest(input), input)
	}

	mutator.shutdown()
      }
    }
  }
}
