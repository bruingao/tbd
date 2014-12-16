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

class ModList[T, U]
    (val head: Mod[ModListNode[T, U]],
     val sorted: Boolean = false,
     val workerId: WorkerId = -1)
  extends AdjustableList[T, U] with Serializable {

  def filter(pred: ((T, U)) => Boolean)
      (implicit c: Context): ModList[T, U] = {
    val memo = new Memoizer[Mod[ModListNode[T, U]]]()

    new ModList(
      mod {
        read(head) {
	  case null => write[ModListNode[T, U]](null)
	  case node => node.filter(pred, memo)
        }
      }
    )
  }

  def flatMap[V, W](f: ((T, U)) => Iterable[(V, W)])
      (implicit c: Context): ModList[V, W] = {
    val memo = new Memoizer[Changeable[ModListNode[V, W]]]()
    val modizer = new Modizer1[ModListNode[V, W]]()

    new ModList(
      modizer(head.id) {
        read(head) {
          case null => write[ModListNode[V, W]](null)
          case node => node.flatMap(f, memo, modizer)
        }
      }
    )
  }

  def join[V](_that: AdjustableList[T, V], condition: ((T, V), (T, U)) => Boolean)
      (implicit c: Context): ModList[T, (U, V)] = {
    assert(_that.isInstanceOf[ModList[T, V]])
    val that = _that.asInstanceOf[ModList[T, V]]

    val memo = new Memoizer[Changeable[ModListNode[T, (U ,V)]]]()

    new ModList(
      mod {
	read(head) {
	  case null => write[ModListNode[T, (U, V)]](null)
	  case node => node.loopJoin(that, memo, condition)
	}
      }
    )
  }

  def map[V, W](f: ((T, U)) => (V, W))
      (implicit c: Context): ModList[V, W] = {
    val memo = new Memoizer[Changeable[ModListNode[V, W]]]()
    val modizer = new Modizer1[ModListNode[V, W]]()

    new ModList(
      modizer(head.id) {
        read(head) {
          case null => write[ModListNode[V, W]](null)
          case node => node.map(f, memo, modizer)
        }
      }, false, workerId
    )
  }

  override def mapValues[V](f: U => V)
      (implicit c: Context): ModList[T, V] = {
    val memo = new Memoizer[Changeable[ModListNode[T, V]]]()
    val modizer = new Modizer1[ModListNode[T, V]]()

    new ModList(
      modizer(head.id) {
        read(head) {
          case null => write[ModListNode[T, V]](null)
          case node => node.mapValues(f, memo, modizer)
        }
      }, sorted
    )
  }

  def merge(that: ModList[T, U], comparator: ((T, U), (T, U)) => Int)
      (implicit c: Context): ModList[T, U] = {
    merge(that, new Memoizer[Changeable[ModListNode[T, U]]](),
	  new Modizer1[ModListNode[T, U]](), comparator)
  }

  def merge
      (that: ModList[T, U],
       memo: Memoizer[Changeable[ModListNode[T, U]]],
       modizer: Modizer1[ModListNode[T, U]],
      comparator: ((T, U), (T, U)) => Int)
      (implicit c: Context): ModList[T, U] = {
    new ModList(
      modizer(head.id) {
	read(head) {
	  case null =>
	    read(that.head) { write(_) }
	  case node =>
	    read(that.head) {
	      case null =>
		write(node)
	      case thatNode =>
		memo(node, thatNode) {
		  node.merge(thatNode, memo, modizer, comparator)
		}
	    }
	}
      }
    )
  }

  override def mergesort (comparator: ((T, U), (T, U) ) => Int)
      (implicit c: Context): ModList[T, U] = {
    val modizer = new Modizer1[ModListNode[T, U]]()
    def mapper(pair: (T, U)) = {
      val tail = modizer(pair._1) {
	write(new ModListNode[T, U](pair, modizer(pair._1 + "null") { write(null) }))
      }

      ("" + pair._1, new ModList(tail))
    }

    val memo = new Memoizer[ModList[T, U]]()

    def reducer(pair1: (String, ModList[T, U]), pair2: (String, ModList[T, U])) = {
      //println("merging " + pair2._2)
      //println(" and " + pair1._2)

      val merged = memo(pair1, pair2) {
        val memoizer = new Memoizer[Changeable[ModListNode[T, U]]]()
	val modizer = new Modizer1[ModListNode[T, U]]()
	pair2._2.merge(pair1._2, memoizer, modizer, comparator)
      }

      //println("merged - " + merged)
      (pair1._1 + pair2._1, merged)
    }

    val mapped = map(mapper)
    val reduced = mapped.reduce(reducer)

    new ModList(
      mod {
        read(reduced) {
	  case null => write(null)
          case (key, list) => read(list.head) { write(_) }
        }
      }, true
    )
  }

  override def quicksort(comparator: ((T, U), (T, U) ) => Int)
      (implicit c: Context): ModList[T, U] = {
    val sorted = mod {
      read(head) {
        case null => write[ModListNode[T, U]](null)
        case node =>
	  node.quicksort(mod { write(null) }, comparator)
      }
    }

    new ModList(sorted, true)
  }

  def reduce(f: ((T, U), (T, U)) => (T, U))
      (implicit c: Context): Mod[(T, U)] = {

    // Each round we need a hasher and a memo, and we need to guarantee that the
    // same hasher and memo are used for a given round during change propagation,
    // even if the first mod of the list is deleted.
    class RoundMemoizer {
      val memo = new Memoizer[(Hasher,
                               Memoizer[Mod[ModListNode[T, U]]],
                               RoundMemoizer)]()

      def getTuple() =
        memo() {
          (new Hasher(2, 4),
           new Memoizer[Mod[ModListNode[T, U]]](),
           new RoundMemoizer())
	}
    }

    def randomReduceList
        (head: ModListNode[T, U],
         nextMod: ModListNode[T, U],
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
            case null => write(halfList.value)
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
        (acc: (T, U),
         node: ModListNode[T, U],
         round: Int,
         hasher: Hasher,
         memo: Memoizer[Mod[ModListNode[T, U]]])
        (implicit c: Context): Changeable[ModListNode[T, U]] = {
      val newAcc = f(acc, node.value)

      if(binaryHash(node.nextMod.id, round, hasher)) {
        val newNextMod = memo(node.nextMod) {
	  mod {
	    read(node.nextMod) {
              case null => write[ModListNode[T, U]](null)
              case next =>
                read(next.nextMod) {
                  case null =>
                    val tail = mod { write[ModListNode[T, U]](null) }
                    write(new ModListNode(next.value, tail))
                  case nextNext =>
	            halfList(next.value, nextNext, round, hasher, memo)
                }
            }
	  }
	}
        write(new ModListNode(newAcc, newNextMod))
      } else {
        read(node.nextMod) {
          case null =>
	    val tail = mod[ModListNode[T, U]] { write(null) }
            write(new ModListNode(newAcc, tail))
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
            case null => write(head.value)
            case next => randomReduceList(head, next, 0, roundMemoizer)
          }
      }
    }
  }

  override def reduceByKey(f: (U, U) => U, comparator: ((T, U), (T, U) ) => Int)
      (implicit c: Context): ModList[T, U] = {
    val sorted = this.quicksort(comparator)
    val memo = new Memoizer[Changeable[ModListNode[T, U]]]()
    new ModList(
      mod {
	read(sorted.head) {
	  case null =>
	    write(null)
	  case node =>
	    memo(node, node.value._1, null) {
	      node.reduceByKey(f, comparator, node.value._1, null.asInstanceOf[U], memo)
	    }
	}
      }, true
    )
  }

  def sortJoin[V](_that: AdjustableList[T, V])
      (implicit c: Context,
          ordering: Ordering[T]): AdjustableList[T, (U, V)] = {
    assert(_that.isInstanceOf[ModList[T, V]])
    val that = _that.asInstanceOf[ModList[T, V]]

    val comp = (pair1: (T, _), pair2: (T, _)) => {
      ordering.compare(pair1._1, pair2._1)
    }


    val thisSorted =
      if (this.sorted)
	this
      else
	this.mergesort(comp)

    val thatSorted =
      if (that.sorted)
	that
      else
	that.mergesort(comp)

    val memo = new Memoizer[Changeable[ModListNode[T, (U, V)]]]()
    new ModList(
      mod {
	read(thisSorted.head) {
	  case null => write(null)
	  case thisHead =>
	    read(thatSorted.head) {
	      case null => write(null)
	      case thatHead =>
		memo(thisHead, thatHead) {
		  thisHead.sortJoinMerge(thatHead, memo)
		}
	    }
	}
      }
    )
  }

  def split(pred: ((T, U)) => Boolean)
      (implicit c: Context): (AdjustableList[T, U], AdjustableList[T, U]) = {
    val memo = new Memoizer[ModListNode.ChangeableTuple[T, U]]()
    val modizer = new Modizer2[ModListNode[T, U], ModListNode[T, U]]()

    val result = modizer(head.id) {
      read2(head) {
	case null =>
	  write2[ModListNode[T, U], ModListNode[T, U]](null, null)
	case node =>
	  memo(node) {
	    node.split(pred, memo, modizer)
	  }
      }
    }

    (new ModList(result._1), new ModList(result._2))
  }

  def toBuffer(mutator: Mutator): Buffer[(T, U)] = {
    val buf = Buffer[(T, U)]()
    var node = mutator.read(head)
    while (node != null) {
      buf += node.value
      node = mutator.read(node.nextMod)
    }

    buf
  }

  override def equals(that: Any): Boolean = {
    that match {
      case thatList: ModList[T, U] => head == thatList.head
      case _ => false
    }
  }

  override def hashCode() = head.hashCode()

  override def toString: String = {
    head.toString
  }
}
