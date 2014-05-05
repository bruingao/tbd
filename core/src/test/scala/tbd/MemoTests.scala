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

import tbd.{Adjustable, Changeable, Mutator, TBD}
import tbd.ddg.{MemoNode, ReadNode, RootNode}
import tbd.mod.{Dest, Mod}

class MemoTest extends Adjustable {
  // Note: real client applications should NOT have mutable state like this.
  // We are just using it to ensure that the memoized function doesn't get
  // reexecuted as appropriate.
  var count = 0

  def run(tbd: TBD): Mod[Int] = {
    val one = tbd.input.get[Mod[Int]](1)
    val two = tbd.input.get[Mod[Int]](2)
    val lift = tbd.makeLift[Mod[Int]]()

    tbd.mod((dest: Dest[Int]) => {
      tbd.read(one)(oneValue => {
        if (oneValue == 3) {
          tbd.mod((dest: Dest[Int]) => {
            tbd.read(one)(oneValueAgain => {
              tbd.write(dest, oneValueAgain)
            })
          })
        }
        val memoMod = lift.memo(List(two), () => {
          tbd.mod((memoDest: Dest[Int]) => {
	    count += 1
	    tbd.read(two)(valueTwo => {
	      tbd.write(memoDest, valueTwo + 1)
	    })
          })
        })

        tbd.read(memoMod)(memoValue => {
          tbd.write(dest, oneValue + memoValue)
        })
      })
    })
  }
}

class AlreadyMatchedTest extends Adjustable {
  var count1 = 0
  var count2 = 0

  /**
   * In the first run, one = 1, so the first call to lift doesn't get
   * executed, so count1 = 0, count2 = 1. In the second run, one = 3, so the
   * read of one is reexecuted, a memo match is found for the first call to
   * memo, but a memo match should not be found for the second call since we
   * can only match a memo entry once, so count1 = 0, count2 = 2.
   *
   * Note: it is generally not advisable to call memo on the same lift object with
   * different parameters for the memoized function.
   */
  def run(tbd: TBD): Mod[Int] = {
    val one = tbd.input.get[Mod[Int]](1)
    val two = tbd.input.get[Mod[Int]](2)
    val lift = tbd.makeLift[Changeable[Int]]()

    tbd.mod((dest: Dest[Int]) => {
      tbd.read(one)(oneValue => {
        if (oneValue == 3) {
          tbd.mod((dest: Dest[Int]) => {
            lift.memo(List(two), () => {
	      count1 += 1
	      tbd.read(two)(twoValue => {
	        tbd.write(dest, twoValue + 2)
	      })
            })
          })
        }

        lift.memo(List(two), () => {
	    count2 += 1
	    tbd.read(two)(twoValue => {
	      tbd.write(dest, twoValue + 1)
          })
        })
      })
    })
  }
}

class OutOfScopeTest extends Adjustable {
  var num = 0

  def run(tbd: TBD): Int = {
    val one = tbd.input.get[Mod[Int]](1)
    val two = tbd.input.get[Mod[Int]](2)
    val lift = tbd.makeLift[Changeable[Int]]()

    tbd.mod((dest: Dest[Int]) => {
      tbd.read(one)(oneValue => {
	if (oneValue != 1) {
	  lift.memo(List(two, dest), () => {
	    num += 1
	    tbd.write(dest, 0)
	  })
	} else {
	  tbd.write(dest, 1)
	}
      })
    })

    tbd.mod((dest: Dest[Int]) => {
      lift.memo(List(two, dest), () => {
	tbd.write(dest, 2)
      })
    })

    0
  }
}

class MatchingSignaturesTest extends Adjustable {
  var count1 = 0
  var count2 = 0

  def run(tbd: TBD): Int = {
    val one = tbd.input.get[Mod[Int]](1)
    val two = tbd.input.get[Mod[Int]](2)
    val lift = tbd.makeLift[Changeable[Int]]()

    tbd.mod((dest: Dest[Int]) => {
      tbd.read(one)(oneValue => {
        lift.memo(List(two, dest), () => {
          count1 += 1
          tbd.write(dest, 0)
        })
      })
    })

    tbd.mod((dest: Dest[Int]) => {
      tbd.read(one)(oneValue => {
        lift.memo(List(two, dest), () => {
          count2 += 1
          tbd.write(dest, 0)
        })
      })
    })

    0
  }
}

class MemoTests extends FlatSpec with Matchers {
  "MemoTest" should "do stuff" in {
    val mutator = new Mutator()
    mutator.put(1, 1)
    mutator.put(2, 10)
    val test = new MemoTest()
    val output = mutator.run[Mod[Int]](test)
    output.read() should be (12)
    test.count should be (1)

    val root = new MockRootNode(List(
      new MockReadNode(List(
        new MockMemoNode(List(
          new MockReadNode(List())
        )),
        new MockReadNode(List())
      ))
    ))

    val ddg = mutator.getDDG()
    assert(root.isEqual(ddg.root))

    // Change the mod not read by the memoized function,
    // check that it isn't called.
    mutator.update(1, 3)
    mutator.propagate()
    output.read() should be (14)
    test.count should be (1)

    val root2 = new MockRootNode(List(
      new MockReadNode(List(
        new MockReadNode(List()),
        new MockMemoNode(List(
          new MockReadNode(List())
        )),
        new MockReadNode(List())
      ))
    ))

    val ddg2 = mutator.getDDG()
    assert(root2.isEqual(ddg2.root))

    // Change the other mod, the memoized function should
    // be called.
    mutator.update(1, 2)
    mutator.update(2, 8)
    mutator.propagate()
    output.read() should be (11)
    test.count should be (2)

    val ddg3 = mutator.getDDG()
    assert(root.isEqual(ddg3.root))

    mutator.shutdown()
  }

  "AlreadyMatchedTest" should "only reuse the memo entry once" in {
    val mutator = new Mutator()
    mutator.put(1, 1)
    mutator.put(2, 10)
    val test = new AlreadyMatchedTest()
    val output = mutator.run[Mod[Int]](test)
    output.read() should be (11)
    test.count1 should be (0)
    test.count2 should be (1)

    mutator.update(1, 3)
    mutator.propagate()
    output.read() should be (11)
    test.count1 should be (0)
    test.count2 should be (2)

    mutator.shutdown()
  }

  "OutOfScopeTest" should "not memo match outside of the reexecuted read" in {
    val mutator = new Mutator()
    mutator.put(1, 1)
    mutator.put(2, 2)
    val test = new OutOfScopeTest()
    mutator.run[Int](test)

    test.num should be (0)

    mutator.update(1, 3)
    mutator.propagate()

    test.num should be (1)

    mutator.shutdown()
  }

  "MatchingSignaturesTest" should "find both memo matches" in {
    val mutator = new Mutator()
    mutator.put(1, 1)
    mutator.put(2, 2)
    val test = new MatchingSignaturesTest()

    mutator.run[Int](test)
    test.count1 should be (1)
    test.count2 should be (1)

    mutator.update(1, 2)
    mutator.propagate()
    test.count1 should be (1)
    test.count2 should be (1)

    mutator.shutdown()
  }
}
