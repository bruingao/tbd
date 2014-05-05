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

import tbd.{Changeable, TBD}
import tbd.memo.Lift

class DoubleModListNode[T](
    aValue: Mod[T],
    aNext: Mod[DoubleModListNode[T]]) {
  val valueMod = aValue
  val next = aNext

  def map[U](
      tbd: TBD,
      dest: Dest[DoubleModListNode[U]],
      f: (TBD, T) => U): Changeable[DoubleModListNode[U]] = {
    val newValue = tbd.mod((dest: Dest[U]) =>
      tbd.read(valueMod)(value => tbd.write(dest, f(tbd, value))))
    val newNext = tbd.mod((dest: Dest[DoubleModListNode[U]]) =>
      tbd.read(next)(next => {
        if (next != null) {
          next.map(tbd, dest, f)
        } else {
          tbd.write(dest, null)
        }
      }))
    tbd.write(dest, new DoubleModListNode[U](newValue, newNext))
  }

  def foldl(
      tbd: TBD,
      dest: Dest[T],
      initialValueMod: Mod[T],
      f: (TBD, T, T) => T): Changeable[T] = {
    tbd.read(initialValueMod)(initialValue => {
      val reducedValueMod = tbd.mod((dest: Dest[T]) => {  
        tbd.read(valueMod)(value =>
          tbd.write(dest, f(tbd, value, initialValue))) 
      })
      tbd.read(next)(next => {
        if(next != null) {
          next.foldl(tbd, dest, reducedValueMod, f)
        } else {
          tbd.read(reducedValueMod)(reducedValue =>
            tbd.write(dest, reducedValue))
        }
      })
    })
  }

  def reducePairs(
      tbd: TBD, 
      dest: Dest[DoubleModListNode[T]],
      f: (TBD, T, T) => T): Changeable[DoubleModListNode[T]] = {
    tbd.read(next)(next => {
      if(next != null) {
        val newValueMod = tbd.mod((dest: Dest[T]) => 
          tbd.read(next.valueMod)(nextValue =>
            tbd.read(valueMod)(value =>
              tbd.write(dest, f(tbd, value, nextValue)))))
        val newNext = tbd.mod((dest: Dest[DoubleModListNode[T]]) =>
          tbd.read(next.next)(nextNext => {
            if(nextNext != null) {
              nextNext.reducePairs(tbd, dest, f)
            } else {
              tbd.write(dest, null)
            }}))
        tbd.write(dest, new DoubleModListNode(newValueMod, newNext))
      } else {
        tbd.write(dest, this)
      }
    })
  }

  def memoMap[W](
      tbd: TBD,
      dest: Dest[DoubleModListNode[W]],
      f: (TBD, T) => W,
      lift: Lift[Mod[DoubleModListNode[W]]]): Changeable[DoubleModListNode[W]] = {
    val newValue = tbd.mod((dest: Dest[W]) =>
      tbd.read(valueMod)(value => tbd.write(dest, f(tbd, value))))
    val newNext = lift.memo(List(next), () => {
      tbd.mod((dest: Dest[DoubleModListNode[W]]) =>
        tbd.read(next)(next => {
          if (next != null) {
            next.memoMap(tbd, dest, f, lift)
          } else {
            tbd.write(dest, null)
          }
        })
      )
    })
    tbd.write(dest, new DoubleModListNode[W](newValue, newNext))
  }

  def parMap[W](
      tbd: TBD,
      dest: Dest[DoubleModListNode[W]],
      f: (TBD, T) => W): Changeable[DoubleModListNode[W]] = {
    val modTuple =
      tbd.par((tbd: TBD) => {
	tbd.mod((valueDest: Dest[W]) => {
          tbd.read(valueMod)(value => {
            tbd.write(valueDest, f(tbd, value))
          })
        })
      }, (tbd: TBD) => {
        tbd.mod((nextDest: Dest[DoubleModListNode[W]]) => {
	  tbd.read(next)(next => {
            if (next != null) {
              next.parMap(tbd, nextDest, f)
            } else {
              tbd.write(nextDest, null)
            }
          })
        })
      })

    tbd.write(dest, new DoubleModListNode[W](modTuple._1, modTuple._2))
  }

  def filter(
      tbd: TBD,
      dest: Dest[DoubleModListNode[T]],
      pred: T => Boolean): Changeable[DoubleModListNode[T]] = {
    tbd.read(valueMod)(value => {
      if (pred(value)) {
        val newNext = tbd.mod((nextDest: Dest[DoubleModListNode[T]]) => {
          tbd.read(next)(nextValue => {
            if (nextValue == null) {
              tbd.write(nextDest, null)
            } else {
              nextValue.filter(tbd, nextDest, pred)
            }
          })
        })
        tbd.write(dest, new DoubleModListNode(valueMod, newNext))
      } else {
        tbd.read(next)(nextValue => {
          if (nextValue == null) {
            tbd.write(dest, null)
          } else {
            nextValue.filter(tbd, dest, pred)
          }
        })
      }
    })
  }

  def memoFilter(
      tbd: TBD,
      dest: Dest[DoubleModListNode[T]],
      pred: T => Boolean,
      lift: Lift[Mod[DoubleModListNode[T]]])
        : Changeable[DoubleModListNode[T]] = {
    tbd.read(valueMod)(value => {
      if (pred(value)) {
        val newNext = lift.memo(List(next), () => {
          tbd.mod((nextDest: Dest[DoubleModListNode[T]]) => {
            tbd.read(next)(nextValue => {
                if (nextValue == null) {
                  tbd.write(nextDest, null)
                } else {
                  nextValue.memoFilter(tbd, nextDest, pred, lift)
                }
              })
            })
          })
        tbd.write(dest, new DoubleModListNode(valueMod, newNext))
      } else {
        tbd.read(next)(nextValue => {
          if (nextValue == null) {
            tbd.write(dest, null)
          } else {
            nextValue.memoFilter(tbd, dest, pred, lift)
          }
        })
      }
    })
  }

  override def toString: String = {
    def toString(lst: DoubleModListNode[T]):String = {
      val nextRead = lst.next.read()
      val next =
	if (nextRead != null)
	  ", " + toString(nextRead)
	else
	  ")"

      lst.valueMod + next
    }

    "DoubleModListNode(" + toString(this)
  }
}
