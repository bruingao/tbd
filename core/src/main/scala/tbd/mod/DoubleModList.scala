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
package tbd.mod

import scala.collection.mutable.{ArrayBuffer, Buffer}

import tbd.{Changeable, TBD}
import tbd.memo.Lift

class DoubleModList[T, V](
    aHead: Mod[DoubleModListNode[T, V]])
    extends AdjustableList[T, V]
    with Iterable[T, V, DoubleModListNode[T, V]] {
  val head = aHead

  def iterators(tbd: TBD) = {
    List(head)
  }

  def map[U, Q](
      tbd: TBD,
      f: (TBD, T, V) => (U, Q),
      parallel: Boolean = false,
      memoized: Boolean = true): DoubleModList[U, Q] = {
    if (parallel) {
      new DoubleModList(
        tbd.mod((dest: Dest[DoubleModListNode[U, Q]]) => {
          tbd.read(head)(node => {
            if (node != null) {
              node.parMap(tbd, dest, f)
            } else {
              tbd.write(dest, null)
            }
          })
        })
      )
    } else {
      val lift = tbd.makeLift[Mod[DoubleModListNode[U, Q]]](!memoized)

      new DoubleModList(
        tbd.mod((dest: Dest[DoubleModListNode[U, Q]]) => {
          tbd.read(head)(node => {
            if (node != null) {
              node.map(tbd, dest, f, lift)
            } else {
              tbd.write(dest, null)
            }
          })
        })
      )
    }
  }

  def randomReduce(
      tbd: TBD,
      initialValueMod: Mod[(T, V)],
      f: (TBD, T, V, T, V) => (T, V),
      parallel: Boolean = false,
      memoized: Boolean = false): Mod[(T, V)] = {
    val zero = 0
    val halfLift = tbd.makeLift[Mod[DoubleModListNode[T, V]]](!memoized)

    tbd.mod((dest: Dest[(T, V)]) => {
      tbd.read(head)(h => {
        if(h == null) {
          tbd.read(initialValueMod)(initialValue =>
            tbd.write(dest, initialValue))
        } else {
          randomReduceList(tbd, initialValueMod,
                             head, zero, dest,
                             halfLift, f)
        }
      })
    })
  }

  def randomReduceList(
      tbd: TBD,
      identityMod: Mod[(T, V)],
      head: Mod[DoubleModListNode[T, V]],
      round: Int,
      dest: Dest[(T, V)],
      lift: Lift[Mod[DoubleModListNode[T, V]]],
      f: (TBD, T, V, T, V) => (T, V)): Changeable[(T, V)] = {


    val halfListMod = lift.memo(List(identityMod, head), () => {
        tbd.mod((dest: Dest[DoubleModListNode[T, V]]) => {
          halfList(tbd, identityMod, identityMod,
                   head, round, new Hasher(2, 8),
                   lift, dest, f)
      })
    })

    tbd.read(halfListMod)(halfList => {
      tbd.read(halfList.next)(next => {
        if(next == null) {
          tbd.read(halfList.value)(value =>
            tbd.write(dest, (halfList.key, value)))
        } else {
            randomReduceList(tbd, identityMod, halfListMod,
                               round + 1, dest,
                               lift, f)
        }
      })
    })
  }

  def binaryHash(id: T, round: Int, hasher: Hasher) = {
    hasher.hash(id.hashCode() ^ round) == 0
  }

  def halfList(
      tbd: TBD,
      identityMod: Mod[(T, V)],
      acc: Mod[(T, V)],
      head: Mod[DoubleModListNode[T, V]],
      round: Int,
      hasher: Hasher,
      lift: Lift[Mod[DoubleModListNode[T, V]]],
      dest: Dest[DoubleModListNode[T, V]],
      f: (TBD, T, V, T, V) => (T, V)): Changeable[DoubleModListNode[T, V]] = {
    tbd.read2(head, acc)((head, acc) => {
      tbd.read2(head.value, head.next)((value, next) => {
        if(next == null) {
            val newValue = tbd.createMod(
                             f(tbd, acc._1, acc._2, head.key, value))
            tbd.read(newValue)(value => {
              val newList = new DoubleModListNode(
                              value._1,
                              tbd.createMod(value._2),
                              tbd.createMod[DoubleModListNode[T, V]](null))
              tbd.write(dest, newList)
            })
        } else {
          if(binaryHash(head.key, round, hasher)) {
            val reducedList = lift.memo(List(head.next, identityMod), () => {
              tbd.mod((dest: Dest[DoubleModListNode[T, V]]) => {
                halfList(tbd, identityMod, identityMod,
                         head.next, round, hasher, lift, dest, f)
              })
            })
            val newValue = f(tbd, acc._1, acc._2, head.key, value)
            val newList =  new DoubleModListNode(newValue._1,
						 tbd.createMod(newValue._2),
						 reducedList)
            tbd.write(dest, newList)
          } else {
            val newAcc = tbd.createMod(f(tbd, acc._1, acc._2, head.key, value))
            halfList(tbd, identityMod, newAcc,
                     head.next, round, hasher, lift, dest, f)
          }
        }
      })
    })
  }

  def reduce(
      tbd: TBD,
      initialValueMod: Mod[(T, V)],
      f: (TBD, T, V, T, V) => (T, V),
      parallel: Boolean = false,
      memoized: Boolean = false): Mod[(T, V)] = {
    randomReduce(tbd, initialValueMod, f)
  }

  def filter(
      tbd: TBD,
      pred: (T, V) => Boolean,
      parallel: Boolean = false,
      memoized: Boolean = true): DoubleModList[T, V] = {
    val lift = tbd.makeLift[Mod[DoubleModListNode[T, V]]](!memoized)

    new DoubleModList(
      tbd.mod((dest: Dest[DoubleModListNode[T, V]]) => {
        tbd.read(head)(node => {
          if (node != null) {
            node.filter(tbd, dest, pred, lift)
          } else {
            tbd.write(dest, null)
          }
        })
      })
    )
  }

  def toBuffer(): Buffer[V] = {
    val buf = ArrayBuffer[V]()
    var node = head.read()
    while (node != null) {
      buf += node.value.read()
      node = node.next.read()
    }

    buf
  }

  override def toString: String = {
    head.toString
  }
}
