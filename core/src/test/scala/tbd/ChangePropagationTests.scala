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

import tbd.{Adjustable, Changeable, Context, ListConf, ListInput, Memoizer, Mutator, TableInput}
import tbd.mod.{AdjustableList, Mod}
import tbd.TBD._

class PropagationOrderTest(input: TableInput[Int, Int])
    extends Adjustable[Mod[Int]] {
  var num = 0

  def run(implicit c: Context) = {
    val table = input.getTable()
    val one = table.get(1)

    mod {
      read(one) {
	case v1 =>
          assert(num == 0)
          num += 1
          read(one) {
	    case v2 =>
              assert(num == 1)
              num += 1
              write(v2)
          }
      }

      read(one) {
	  case v3 =>
          assert(num == 2)
          write(v3)
      }
    }
  }
}

class PropagationOrderTest2(input: ListInput[Int, Int])
    extends Adjustable[AdjustableList[Int, Int]] {
  val values = ArrayBuffer[Int]()

  def run(implicit c: Context) = {
    val adjustableList = input.getAdjustableList()
    adjustableList.map((pair: (Int, Int)) => {
      if (c.initialRun) {
        values += pair._2
      } else {
        assert(pair._2 == values.head + 1)
        values -= values.head
      }
      pair
    })
  }
}

class ReduceTest(input: ListInput[Int, Int])
    extends Adjustable[Mod[(Int, Int)]] {
  def run(implicit c: Context) = {
    val list = input.getAdjustableList()
    val zero = createMod((0, 0))
    list.reduce(zero, (pair1: (Int, Int), pair2: (Int, Int)) => {
      //println("reducing " + pair1._2 + " " + pair2._2)
      (pair1._1, pair1._2 + pair2._2)
    })
  }
}

class ParTest(input: TableInput[Int, Int]) extends Adjustable[Mod[Int]] {
  def run(implicit c: Context) = {
    val table = input.getTable()
    val one = table.get(1)

    val pair = par {
      c =>
	mod {
          read(one) {
            case oneValue => write(oneValue + 1)(c)
	  } (c)
	} (c)
    } and {
      c => 0
    }

    mod {
      read(pair._1) {
	case value => write(value * 2)
      }
    }
  }
}

// Checks that modNoDest returns the correct values even after a convuloted
// series of memo matches.
class ModNoDestTest(input: TableInput[Int, Int])
    extends Adjustable[Mod[Int]] {
  val table = input.getTable()
  val one = table.get(1)
  val two = table.get(2)
  val three = table.get(3)
  val four = table.get(4)
  val five = table.get(5)
  val six = table.get(6)
  val seven = table.get(7)

  // If four == 4, twoMemo returns 6, otherwise it returns sevenValue.
  def twoMemo(memo: Memoizer[Changeable[Int]])(implicit c: Context) = {
    read(four)(fourValue => {
      if (fourValue == 4) {
	mod {
	  memo(five) {
	    read(seven)(sevenValue => {
	      write(sevenValue)
	    })
	  }
	}

	memo(six) {
	  write(6)
	}
      } else {
	memo(five) {
	  read(seven)(sevenValue => {
	    write(sevenValue)
	  })
	}
      }
    })
  }

  def run(implicit c: Context) = {
    val memo = makeMemoizer[Changeable[Int]]()

    mod {
      read(one)(oneValue => {
	if (oneValue == 1) {
	  val mod1 = mod {
	    memo(two) {
	      twoMemo(memo)
	    }
	  }

	  read(mod1)(value1 => {
	    write(value1)
	  })
	} else {
	  memo(two) {
	    twoMemo(memo)
	  }
	}
      })
    }
  }
}

class ChangePropagationTests extends FlatSpec with Matchers {
  "PropagationOrderTest" should "reexecute reads in the correct order" in {
    val mutator = new Mutator()
    val input = mutator.createTable[Int, Int]()
    input.put(1, 1)
    val test = new PropagationOrderTest(input)
    val output = mutator.run(test)
    test.num should be (2)

    test.num = 0
    input.update(1, 2)
    mutator.propagate()
    test.num should be (2)
  }

  "PropagationOrderTest2" should "reexecute map in the correct order" in {
    val mutator = new Mutator()
    val input = mutator.createList[Int, Int](new ListConf(partitions = 1))

    for (i <- 0 to 100) {
      input.put(i, i)
    }
    val test = new PropagationOrderTest2(input)
    mutator.run(test)

    for (i <- 0 to 100) {
      input.update(i, i + 1)
    }

    mutator.propagate()

    mutator.shutdown()
  }

  "ReduceTest" should "reexecute only the necessary reduce steps" in {
    val mutator = new Mutator()
    val input = mutator.createList[Int, Int](new ListConf(partitions = 1))
    for (i <- 1 to 16) {
      input.put(i, i)
    }
    val test = new ReduceTest(input)
    val output = mutator.run(test)

    output.read()._2 should be (136)

    input.remove(16)
    input.remove(7)
    mutator.propagate()

    output.read()._2 should be (113)
  }

  "ParTest" should "do something" in {
    val mutator = new Mutator()
    val input = mutator.createTable[Int, Int]()
    input.put(1, 1)
    val output = mutator.run(new ParTest(input))
    output.read() should be (4)

    input.update(1, 2)
    mutator.propagate()

    output.read() should be (6)

    mutator.shutdown()
  }

  "ModNoDestTest" should "update the dests for the memo matches" in {
    val mutator = new Mutator()
    val input = mutator.createTable[Int, Int]()
    input.put(1, 1)
    input.put(2, 2)
    input.put(3, 3)
    input.put(4, 4)
    input.put(5, 5)
    input.put(6, 6)
    input.put(7, 7)
    val output = mutator.run(new ModNoDestTest(input))
    output.read() should be (6)

    input.update(1, 2)
    input.update(4, 5)
    mutator.propagate()
    output.read() should be (7)

    input.update(1, 1)
    input.update(7, 8)
    mutator.propagate()
    output.read() should be (8)

    mutator.shutdown()
  }
}
