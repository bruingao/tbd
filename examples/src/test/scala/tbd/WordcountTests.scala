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
import scala.collection.mutable.Map

import tbd.Mutator
import tbd.examples.list.WCAdjust
import tbd.mod.Mod

class WordcountTests extends FlatSpec with Matchers {
  "WCAdjust" should "return the corrent word count" in {
    val mutator = new Mutator()
    mutator.put(1, "apple boy apple cat cat cat")
    mutator.put(2, "cat boy boy ear cat dog")
    mutator.put(3, "ear cat apple")

    val output = mutator.run[Mod[(Int, Map[String, Int])]](new WCAdjust(2, false))
    val answer = Map[String, Int]("apple" -> 3, "boy" -> 3, "cat" -> 6,
                                  "dog" -> 1, "ear" -> 2)
    assert(output.read()._2 == answer)


    mutator.update(1, "apple dog dog dog face")
    mutator.propagate()
    val answer2 = Map[String, Int]("apple" -> 2, "boy" -> 2, "cat" -> 3,
                                   "dog" -> 4, "ear" -> 2, "face" -> 1)
    assert(output.read()._2 == answer2)

    mutator.put(4, "cat apple")
    mutator.propagate()
    val answer3 = Map[String, Int]("apple" -> 3, "boy" -> 2, "cat" -> 4,
                                   "dog" -> 4, "ear" -> 2, "face" -> 1)
    assert(output.read()._2 == answer3)

    mutator.update(4, "boy dog")
    mutator.put(5, "girl girl girl apple")
    mutator.propagate()
    val answer4 = Map[String, Int]("apple" -> 3, "boy" -> 3, "cat" -> 3,
                                   "dog" -> 5, "ear" -> 2, "face" -> 1,
                                   "girl" -> 3)
    assert(output.read()._2 == answer4)

    mutator.shutdown()
  }
}
