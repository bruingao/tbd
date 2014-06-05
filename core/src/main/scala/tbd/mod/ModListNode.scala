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

class ModListNode[T, V] (
    _value: (T, V),
    _next: Mod[ModListNode[T, V]]) {
  var value = _value
  def next = _next

  def map[U, Q](
      tbd: TBD,
      dest: Dest[ModListNode[U, Q]],
      f: (TBD, (T, V)) => (U, Q),
      lift: Lift[Mod[ModListNode[U, Q]]]
      ): Changeable[ModListNode[U, Q]] = {
    val newNext = //lift.memo(List(next), () => {
      //tbd.mod((dest: Dest[ModListNode[U, Q]]) =>
        tbd.readMod(next, lift)((dest: Dest[ModListNode[U, Q]], next: ModListNode[T, V]) => {
          if (next != null) {
            next.map(tbd, dest, f, lift)
          } else {
            tbd.write(dest, null)
          }
        })
      //)
    //})
    tbd.write(dest, new ModListNode[U, Q](f(tbd, value), newNext))
  }

  def parMap[U, Q](
      tbd: TBD,
      dest: Dest[ModListNode[U, Q]],
      f: (TBD, (T, V)) => (U, Q)): Changeable[ModListNode[U, Q]] = {
    val modTuple =
      tbd.par((tbd: TBD) => {
	f(tbd, value)
      }, (tbd: TBD) => {
        tbd.mod((nextDest: Dest[ModListNode[U, Q]]) => {
	        tbd.read(next)(next => {
            if (next != null) {
              next.parMap(tbd, nextDest, f)
            } else {
              tbd.write(nextDest, null)
            }
          })
        })
      })
    tbd.write(dest, new ModListNode[U, Q](modTuple._1, modTuple._2))
  }

  /*def filter(
      tbd: TBD,
      dest: Dest[ModListNode[T, V]],
      pred: ((T, V)) => Boolean,
      lift: Lift[Mod[ModListNode[T, V]]])
        : Changeable[ModListNode[T, V]] = {
    tbd.read(value)(value => {
      if (pred(value)) {
        val newNext = lift.memo(List(next), () => {
          tbd.mod((nextDest: Dest[ModListNode[T, V]]) => {
            tbd.read(next)(nextValue => {
                if (nextValue == null) {
                  tbd.write(nextDest, null)
                } else {
                  nextValue.filter(tbd, nextDest, pred, lift)
                }
              })
            })
          })
        tbd.write(dest, new ModListNode(tbd.createMod(value), newNext))
      } else {
        tbd.read(next)(nextValue => {
          if (nextValue == null) {
            tbd.write(dest, null)
          } else {
            nextValue.filter(tbd, dest, pred, lift)
          }
        })
      }
    })
  }*/

  override def toString: String = {
    def toString(lst: ModListNode[T, V]):String = {
      val nextRead = lst.next.read()
      val next =
	if (nextRead != null)
	  ", " + toString(nextRead)
	else
	  ")"

      "key=" + lst.value._1 + " next=" + lst.next.id + next
    }

    "ModListNode(" + toString(this)
  }
}
