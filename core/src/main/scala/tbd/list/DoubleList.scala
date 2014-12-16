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
package tbd.list

import java.io.Serializable
import scala.collection.mutable.{Buffer, Map}

import tbd._
import tbd.Constants._
import tbd.TBD._

class DoubleList[T, U]
    (val head: Mod[DoubleListNode[T, U]],
     val sorted: Boolean = false,
     val workerId: WorkerId = -1)
  extends AdjustableList[T, U] with Serializable {

  def filter(pred: ((T, U)) => Boolean)
      (implicit c: Context): DoubleList[T, U] = ???

  def flatMap[V, W](f: ((T, U)) => Iterable[(V, W)])
      (implicit c: Context): DoubleList[V, W] = ???

  def join[V](_that: AdjustableList[T, V], condition: ((T, V), (T, U)) => Boolean)
      (implicit c: Context): DoubleList[T, (U, V)] = ???

  def map[V, W](f: ((T, U)) => (V, W))
      (implicit c: Context): DoubleList[V, W] = {
    println("double list map")
    val memo = new Memoizer[Mod[DoubleListNode[V, W]]]()
    val modizer = new Modizer1[DoubleListNode[V, W]]()

    new DoubleList(
      modizer(head.id) {
        read(head) {
          case null => write[DoubleListNode[V, W]](null)
          case node => node.map(f, memo, modizer)
        }
      }, false, workerId
    )
  }

  def merge(that: DoubleList[T, U])
      (implicit c: Context,
       ordering: Ordering[T]): DoubleList[T, U] = ???

  def merge
      (that: DoubleList[T, U],
       memo: Memoizer[Changeable[DoubleListNode[T, U]]],
       modizer: Modizer1[DoubleListNode[T, U]])
      (implicit c: Context,
       ordering: Ordering[T]): DoubleList[T, U] = ???

  def reduce(f: ((T, U), (T, U)) => (T, U))
      (implicit c: Context): Mod[(T, U)] = {
    println("doubleList reduce")
    // Each round we need a hasher and a memo, and we need to guarantee that the
    // same hasher and memo are used for a given round during change
    // propagation, even if the first mod of the list is deleted.
    class RoundMemoizer {
      val memo = new Memoizer[(Hasher,
                               Memoizer[Mod[DoubleListNode[T, U]]],
                               RoundMemoizer)]()

      def getTuple() =
        memo() {
          (new Hasher(2, 4),
           new Memoizer[Mod[DoubleListNode[T, U]]](),
           new RoundMemoizer())
        }
    }

    def randomReduceList
        (head: DoubleListNode[T, U],
         nextMod: DoubleListNode[T, U],
         round: Int,
         roundMemoizer: RoundMemoizer)
        (implicit c: Context): Changeable[(T, U)] = {
      val tuple = roundMemoizer.getTuple()

      val halfListMod = mod {
        halfList(head.value, nextMod, round, tuple._1, tuple._2)
      }

      read(halfListMod) {
        case halfList =>
          read(halfList.nextMod) {
            case null => read(halfList.value) { case value => write(value) }
            case next => randomReduceList(halfList, next, round + 1, tuple._3)
          }
      }
    }

    def binaryHash(id: ModId, round: Int, hasher: Hasher) = {
      hasher.hash(id.hashCode() ^ round) == 0

      // makes reduce deterministic, for testing purposes
      // id.hashCode() % 3 == 0
    }

    def halfList
        (acc: Mod[(T, U)],
         node: DoubleListNode[T, U],
         round: Int,
         hasher: Hasher,
         memo: Memoizer[Mod[DoubleListNode[T, U]]])
        (implicit c: Context): Changeable[DoubleListNode[T, U]] = {
      val newAcc = mod {
        read_2(acc, node.value) {
          case (acc, value) => write(f(acc, value))
        }
      }

      if(binaryHash(node.nextMod.id, round, hasher)) {
        val newNextMod = memo(node.nextMod) {
          mod {
            read(node.nextMod) {
              case null => write[DoubleListNode[T, U]](null)
              case next =>
                read(next.nextMod) {
                  case null =>
                    val tail = mod { write[DoubleListNode[T, U]](null) }
                    write(new DoubleListNode(next.value, tail))
                  case nextNext =>
                    halfList(next.value, nextNext, round, hasher, memo)
                }
            }
          }
        }
        write(new DoubleListNode(newAcc, newNextMod))
      } else {
        read(node.nextMod) {
          case null =>
            val tail = mod[DoubleListNode[T, U]] { write(null) }
            write(new DoubleListNode(newAcc, tail))
          case next =>
            halfList(newAcc, next, round, hasher, memo)
        }
      }
    }

    val roundMemoizer = new RoundMemoizer()
    mod {
      read(head) {
        case null => write(null)
        case head =>
          read(head.nextMod) {
            case null => read(head.value) { value => write(value) }
            case next => randomReduceList(head, next, 0, roundMemoizer)
          }
      }
    }
  }

  def sortJoin[V](_that: AdjustableList[T, V])
      (implicit c: Context,
       ordering: Ordering[T]): AdjustableList[T, (U, V)] = ???

  def split(pred: ((T, U)) => Boolean)
      (implicit c: Context): (AdjustableList[T, U], AdjustableList[T, U]) = ???

  def toBuffer(mutator: Mutator): Buffer[(T, U)] = {
    val buf = Buffer[(T, U)]()
    var node = mutator.read(head)
    while (node != null) {
      buf += mutator.read(node.value)
      node = mutator.read(node.nextMod)
    }

    buf
  }

  override def equals(that: Any): Boolean = {
    that match {
      case thatList: DoubleList[T, U] => head == thatList.head
      case _ => false
    }
  }

  override def hashCode() = head.hashCode()

  override def toString: String = {
    head.toString
  }
}
