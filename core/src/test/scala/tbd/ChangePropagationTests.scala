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
import scala.collection.mutable.ArrayBuffer

import tbd.{Adjustable, Changeable, Mutator, TBD}
import tbd.mod.{Dest, Mod, AdjustableList}

class PropagationOrderTest extends Adjustable {
  var num = 0

  def run(tbd: TBD): Mod[Int] = {
    val one = tbd.input.getMod[Int](1)

    tbd.mod((dest: Dest[Int]) => {
      tbd.read(one)(v1 => {
        assert(num == 0)
        num += 1
        tbd.read(one)(v2 => {
          assert(num == 1)
          num += 1
          tbd.write(dest, v2)
        })
      })

      tbd.read(one)(v3 => {
        assert(num == 2)
        tbd.write(dest, v3)
      })
    })
  }
}

class PropagationOrderTest2 extends Adjustable {
  val values = ArrayBuffer[Int]()

  def run(tbd: TBD): AdjustableList[Int, Int] = {
    val adjustableList = tbd.input.getAdjustableList[Int, Int](partitions = 1)
    adjustableList.map(tbd, (tbd: TBD, key: Int, value: Int) => {
      if (tbd.initialRun) {
        values += value
      } else {
        assert(value == values.head + 1)
        values -= values.head
      }
      (key, value)
    })
  }
}

class ChangePropagationTests extends FlatSpec with Matchers {
  "PropagationOrderTest" should "reexecute reads in the correct order" in {
    val mutator = new Mutator()
    mutator.put(1, 1)
    val test = new PropagationOrderTest()
    val output = mutator.run[Mod[Int]](test)
    test.num should be (2)

    test.num = 0
    mutator.update(1, 2)
    mutator.propagate()
    test.num should be (2)
  }

  "PropagationOrderTest2" should "reexecute map in the correct order" in {
    val mutator = new Mutator()
    for (i <- 0 to 100) {
      mutator.put(i, i)
    }
    val test = new PropagationOrderTest2()
    mutator.run[AdjustableList[Int, String]](test)

    for (i <- 0 to 100) {
      mutator.update(i, i + 1)
    }

    mutator.propagate()

    mutator.shutdown()
  }
}
