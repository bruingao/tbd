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

import tbd.{Adjustable, Mutator, TBD}
import tbd.mod.AdjustableList

class MemoryExperiment extends Adjustable {
  def run(tbd: TBD): AdjustableList[Int, Int] = {
    val list = tbd.input.getAdjustableList[Int, Int]()
    list.map(tbd, (tbd: TBD, value: Int, key: Int) => (value, key))
  }
}

object MemoryExperiment {
  def main(args: Array[String]) {
    val max = 1000000
    val mutator = new Mutator()
    for (i <- 0 to max) {
      mutator.put(i, i)
    }

    val output = mutator.run[AdjustableList[Int, Int]](new MemoryExperiment)

    val rand = new scala.util.Random()
    while (true) {
      for (i <- 0 to 100) {
	mutator.update(rand.nextInt(max), rand.nextInt(10))
      }

      mutator.propagate()
    }
  }
}
