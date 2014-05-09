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

class DoubleModList[T](
    aHead: Mod[DoubleModListNode[T]]) extends AdjustableList[T] {
  val head = aHead

  def map[U](
      tbd: TBD,
      f: (TBD, T) => U,
      parallel: Boolean = false,
      memoized: Boolean = true): DoubleModList[U] = {
    if (parallel) {
      new DoubleModList(
        tbd.mod((dest: Dest[DoubleModListNode[U]]) => {
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
      if (memoized) {
        val lift = tbd.makeLift[Mod[DoubleModListNode[U]]]()

        new DoubleModList(
          tbd.mod((dest: Dest[DoubleModListNode[U]]) => {
            tbd.read(head)(node => {
              if (node != null) {
                node.memoMap(tbd, dest, f, lift)
              } else {
                tbd.write(dest, null)
              }
            })
          })
        )
      } else {
        new DoubleModList(
          tbd.mod((dest: Dest[DoubleModListNode[U]]) => {
            tbd.read(head)(node => {
              if (node != null) {
                node.map(tbd, dest, f)
              } else {
                tbd.write(dest, null)
              }
            })
          })
        )
      }
    }
  }

  def foldl(
      tbd: TBD,
      initialValueMod: Mod[T],
      f: (TBD, T, T) => T): Mod[T] = {
    tbd.mod((dest: Dest[T]) => {
      tbd.read(head)(node => {
        if(node != null) {
          node.foldl(tbd, dest, initialValueMod, f)
        } else {
          tbd.read(initialValueMod)(initialValue =>
            tbd.write(dest, initialValue))
        }
      })
    })
  }

  type IdListNode = DoubleModListNode[(ModId, T)]

  def randomReduce(
      tbd: TBD,
      initialValueMod: Mod[T],
      f: (TBD, T, T) => T): Mod[T] = {
    val idListMod = toIdList(tbd, head)
    val zero = tbd.createMod(0)
    val halfLift = tbd.makeLift[Mod[IdListNode]]()
    
    tbd.mod((dest: Dest[T]) => {
      tbd.read(idListMod)(idList => {
        if(idList == null) {
          tbd.read(initialValueMod)(initialValue => 
            tbd.write(dest, initialValue))
        } else {
          randomReduceIdList(tbd, initialValueMod, 
                             idListMod, zero, dest, 
                             halfLift, f)
        }
      })
    }) 
 }

  def toIdList(
      tbd: TBD,  
      head: Mod[DoubleModListNode[T]]): Mod[IdListNode] = {
    tbd.mod((dest: Dest[IdListNode]) => {
      tbd.read(head)(head => {
        if(head == null) {
          tbd.write(dest, null)
        } else {
          tbd.read(head.valueMod)(value => {
            val newValue = tbd.createMod((head.valueMod.id, value))
            val newList = toIdList(tbd, head.next)
            tbd.write(dest, new IdListNode(newValue, newList))
          })
        }
      })
    })
  }

  def randomReduceIdList(
      tbd: TBD,
      identityMod: Mod[T],
      head: Mod[IdListNode],
      round: Mod[Int],
      dest: Dest[T],
      lift: Lift[Mod[IdListNode]],
      f: (TBD, T, T) => T): Changeable[T] = {
    
    val halfListMod = lift.memo(List(identityMod, head, round), () => {
        tbd.mod((dest: Dest[IdListNode]) => {
          halfIdList(tbd, identityMod, identityMod, head, round, dest, f)
      })
    })

    tbd.read(halfListMod)(halfList => {
      tbd.read(halfList.next)(next => {  
        if(next == null) {
          tbd.read(halfList.valueMod)(value => 
            tbd.write(dest, value._2))
        } else {
            randomReduceIdList(tbd, identityMod, halfListMod,
                               tbd.increment(round), dest,
                               lift, f)
        }
      })
    })
  }

  val hasher = new Hasher(2, 8)

  def halfIdList(
      tbd: TBD,
      identityMod: Mod[T],
      acc: Mod[T], 
      head: Mod[IdListNode],
      roundMod: Mod[Int],
      dest: Dest[IdListNode],
      f: (TBD, T, T) => T): Changeable[IdListNode] = {
    tbd.read(head)(head => {
      tbd.read2(head.valueMod, head.next)((value, next) => {
        tbd.read2(acc, roundMod)((acc, round) => { 
          if(next == null) {
              val newValue = tbd.createMod((value._1, f(tbd, acc, value._2)))
              val newList =  new DoubleModListNode(newValue, tbd.createMod[IdListNode](null))
              tbd.write(dest, newList)
          } else {
            if(hasher.binaryHash(value._1, round)) {
              val reducedList = tbd.mod((dest: Dest[IdListNode]) => {
                halfIdList(tbd, identityMod, identityMod, head.next, roundMod, dest, f)
              })
              val newValue = tbd.createMod((value._1, f(tbd, acc, value._2)))
              val newList =  new DoubleModListNode(newValue, reducedList)
              tbd.write(dest, newList)
            } else {
              val newAcc = tbd.createMod(f(tbd, acc, value._2))
              halfIdList(tbd, identityMod, newAcc, head.next, roundMod, dest, f)   
            }
          }
        })
      })
    })
  }
  
  def reduce(
      tbd: TBD,
      initialValueMod: Mod[T],
      f: (TBD, T, T) => T): Mod[T] = {
    randomReduce(tbd, initialValueMod, f)
  }

  def filter(
      tbd: TBD,
      pred: T => Boolean,
      parallel: Boolean = false,
      memoized: Boolean = true): DoubleModList[T] = {
    if (memoized) {
      val lift = tbd.makeLift[Mod[DoubleModListNode[T]]]()

      new DoubleModList(
        tbd.mod((dest: Dest[DoubleModListNode[T]]) => {
          tbd.read(head)(node => {
            if (node != null) {
              node.memoFilter(tbd, dest, pred, lift)
            } else {
              tbd.write(dest, null)
            }
          })
        })
      )
    } else {
      new DoubleModList(
        tbd.mod((dest: Dest[DoubleModListNode[T]]) => {
          tbd.read(head)(node => {
            if (node != null) {
              node.filter(tbd, dest, pred)
            } else {
              tbd.write(dest, null)
            }
          })
        })
      )
    }
  }

  def toBuffer(): Buffer[T] = {
    val buf = ArrayBuffer[T]()
    var node = head.read()
    while (node != null) {
      buf += node.valueMod.read()
      node = node.next.read()
    }

    buf
  }

  override def toString: String = {
    head.toString
  }
}
