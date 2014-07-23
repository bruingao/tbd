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

import java.io.Serializable

import tbd.{Changeable, Changeable2, Context, Memoizer}
import tbd.TBD._

class ModListNode[T, U] (
    var value: (T, U),
    val next: Mod[ModListNode[T, U]]
  ) extends Serializable {

  override def equals(obj: Any): Boolean = {
    if (!obj.isInstanceOf[ModListNode[T, U]]) {
      false
    } else {
      val that = obj.asInstanceOf[ModListNode[T, U]]
      that.value == value && that.next == next
    }
  }

  def filter(
      pred: ((T, U)) => Boolean,
      memo: Memoizer[Mod[ModListNode[T, U]]])
     (implicit c: Context): Changeable[ModListNode[T, U]] = {
    def readNext = {
      read(next) {
	case null => write[ModListNode[T, U]](null)
	case next => next.filter(pred, memo)
      }
    }

    if (pred(value)) {
      val newNext = memo(List(next)) {
        mod {
	  readNext
        }
      }
      write(new ModListNode(value, newNext))
    } else {
      readNext
    }
  }

  def map[V, W](
      f: ((T, U)) => (V, W),
      memo: Memoizer[Changeable[ModListNode[V, W]]])
     (implicit c: Context): Changeable[ModListNode[V, W]] = {
    val newNext = mod {
      read(next) {
	case null =>
	  write[ModListNode[V, W]](null)
	case next =>
          memo(next) {
            next.map(f, memo)
          }
      }
    }

    write(new ModListNode[V, W](f(value), newNext))
  }

  def sort(
      toAppend: Mod[ModListNode[T, U]],
      comparator: ((T, U), (T, U)) => Boolean,
      memo: Memoizer[Mod[ModListNode[T, U]]])
     (implicit c: Context): Changeable[ModListNode[T, U]] = {
    read(next)(next => {
      if(next != null) {
        val (smaller, greater) = mod2(2) {

          val memo = makeMemoizer[Changeable2[ModListNode[T, U], ModListNode[T, U]]]()

          next.split(memo, (cv: (T, U)) => { comparator(cv, value) })
        }

        val greaterSorted = memo(List(greater)) {
          mod {
            read(greater)(greater => {
              if(greater != null) {
                greater.sort(toAppend, comparator, memo)
              } else {
                read(toAppend)(toAppend => {
                  write(toAppend)
                })
              }
            })
          }
        }

        val mid = new ModListNode(value, greaterSorted)

        read(smaller)(smaller => {
          if(smaller != null) {
            smaller.sort(createMod(mid), comparator, memo)
          } else {
            write(mid)
          }
        })
      } else {
        write(new ModListNode(value, toAppend))
      }
    })
  }

  def split(
      memo: Memoizer[Changeable2[ModListNode[T, U], ModListNode[T, U]]],
      pred: ((T, U)) => Boolean)
     (implicit c: Context): Changeable2[ModListNode[T, U], ModListNode[T, U]] = {
    def readNext(next: ModListNode[T, U]) = {
      memo(next) {
	if (next != null) {
	  next.split(memo, pred)
	} else {
	  write2(null.asInstanceOf[ModListNode[T, U]],
		 null.asInstanceOf[ModListNode[T, U]])
	}
      }
    }

    if(pred(value)) {
      val (matchNext, diffNext) =
	mod2(0) {
	  read(next)(readNext)
	}

      writeLeft(new ModListNode(value, matchNext), diffNext)
    } else {
      val (matchNext, diffNext) =
	mod2(1) {
	  read(next)(readNext)
	}

      writeRight(matchNext, new ModListNode(value, diffNext))
    }
  }
}
