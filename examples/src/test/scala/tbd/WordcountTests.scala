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
package tbd.examples.test

import org.scalatest._
import scala.collection.immutable.HashMap

import tbd.{Adjustable, Context, ListInput, Mutator}
import tbd.examples.list.WCAlgorithm
import tbd.mod.Mod
import tbd.TBD._

class WCTest(input: ListInput[Int, String])
    extends Adjustable[Mod[(Int, HashMap[String, Int])]] {
  def mapper(pair: (Int, String)) = {
    (pair._1, WCAlgorithm.wordcount(pair._2))
  }

  def reducer(
      pair1: (Int, HashMap[String, Int]),
      pair2: (Int, HashMap[String, Int])) = {
    (pair1._1, WCAlgorithm.reduce(pair1._2, pair2._2))
   }

  def run(implicit c: Context) = {
    val pages = input.getAdjustableList()
    val counts = pages.map(mapper)
    val initialValue = createMod((0, HashMap[String, Int]()))
    counts.reduce(initialValue, reducer)
  }
}

class WordcountTests extends FlatSpec with Matchers {
  "WCAdjust" should "return the corrent word count" in {
    val mutator = new Mutator()
    val input = mutator.createList[Int, String]()
    val test = new WCTest(input)

    input.put(1, "apple boy apple cat cat cat")
    input.put(2, "cat boy boy ear cat dog")
    input.put(3, "ear cat apple")

    val output = mutator.run(test)
    val answer = HashMap[String, Int]("apple" -> 3, "boy" -> 3, "cat" -> 6,
				      "dog" -> 1, "ear" -> 2)
    assert(output.read()._2 == answer)

    input.update(1, "apple dog dog dog face")
    mutator.propagate()
    val answer2 = HashMap[String, Int]("apple" -> 2, "boy" -> 2, "cat" -> 3,
				       "dog" -> 4, "ear" -> 2, "face" -> 1)
    assert(output.read()._2 == answer2)

    input.put(4, "cat apple")
    mutator.propagate()
    val answer3 = HashMap[String, Int]("apple" -> 3, "boy" -> 2, "cat" -> 4,
				       "dog" -> 4, "ear" -> 2, "face" -> 1)
    assert(output.read()._2 == answer3)

    input.update(4, "boy dog")
    input.put(5, "girl girl girl apple")
    mutator.propagate()
    val answer4 = HashMap[String, Int]("apple" -> 3, "boy" -> 3, "cat" -> 3,
				       "dog" -> 5, "ear" -> 2, "face" -> 1,
				       "girl" -> 3)
    assert(output.read()._2 == answer4)

    mutator.shutdown()
  }
}
