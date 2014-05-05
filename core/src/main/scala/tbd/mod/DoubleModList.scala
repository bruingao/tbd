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

  def reduce(
      tbd: TBD,
      initialValueMod: Mod[T],
      f: (TBD, T, T) => T): Mod[T] = {
    tbd.mod((dest: Dest[T]) =>
      reduceHelper(tbd, initialValueMod, head, dest, f))
  }

  def reduceHelper(
      tbd: TBD,
      initialValueMod: Mod[T],
      head: Mod[DoubleModListNode[T]],
      dest: Dest[T],
      f: (TBD, T, T) => T): Changeable[T] = {
    tbd.read(head)(head => {
      if(head != null) {
        tbd.read(head.next)(next => {
          if(next != null) {
            val newListMod = tbd.mod((dest: Dest[DoubleModListNode[T]]) => {
              head.reducePairs(tbd, dest, f)
            })
            reduceHelper(tbd, initialValueMod, newListMod, dest, f)
          } else {
            tbd.read(head.valueMod)(value =>
              tbd.read(initialValueMod)(initialValue =>
                tbd.write(dest, f(tbd, value, initialValue))))
          }
        })
      } else {
        tbd.read(initialValueMod)(initialValue =>
          tbd.write(dest, initialValue))
      }
    })
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
