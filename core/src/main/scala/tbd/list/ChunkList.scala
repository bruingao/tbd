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
import scala.collection.mutable.Buffer

import tbd._
import tbd.Constants._
import tbd.TBD._

class ChunkList[T, U]
    (val head: Mod[ChunkListNode[T, U]],
     conf: ListConf,
     val workerId: WorkerId = -1)
  extends AdjustableList[T, U] with Serializable {

  override def chunkMap[V, W](f: (Vector[(T, U)]) => (V, W))
      (implicit c: Context): ModList[V, W] = {
    val memo = new Memoizer[Mod[ModListNode[V, W]]]()

    new ModList(
      mod {
        read(head) {
          case null => write[ModListNode[V, W]](null)
          case node => node.chunkMap(f, memo)
        }
      }, false, workerId
    )
  }

  def filter(pred: ((T, U)) => Boolean)
      (implicit c: Context): ChunkList[T, U] = ???

  def flatMap[V, W](f: ((T, U)) => Iterable[(V, W)])
      (implicit c: Context): ChunkList[V, W] = {
    val memo = new Memoizer[Changeable[ChunkListNode[V, W]]]()

    new ChunkList(
      mod {
        read(head) {
          case null => write[ChunkListNode[V, W]](null)
          case node => node.flatMap(f, conf.chunkSize, memo)
        }
      }, conf, workerId
    )
  }

  def join[V](_that: AdjustableList[T, V], condition: ((T, V), (T, U)) => Boolean)
      (implicit c: Context): ChunkList[T, (U, V)] = {
    assert(_that.isInstanceOf[ChunkList[T, V]])
    val that = _that.asInstanceOf[ChunkList[T, V]]

    val memo = new Memoizer[Changeable[ChunkListNode[T, (U ,V)]]]()

    new ChunkList(
      mod {
	read(head) {
	  case null => write[ChunkListNode[T, (U, V)]](null)
	  case node => node.loopJoin(that, memo, condition)
	}
      }, conf
    )
  }

  def keyedChunkMap[V, W](f: (Vector[(T, U)], ModId) => (V, W))
      (implicit c: Context): ModList[V, W] = {
    val memo = new Memoizer[Mod[ModListNode[V, W]]]()

    new ModList(
      mod {
        read(head) {
	  case null => write[ModListNode[V, W]](null)
	  case node => node.keyedChunkMap(f, memo, head.id)
        }
      }
    )
  }

  def map[V, W](f: ((T, U)) => (V, W))
      (implicit c: Context): ChunkList[V, W] = {
    val memo = new Memoizer[Changeable[ChunkListNode[V, W]]]()

    new ChunkList(
      mod {
        read(head) {
          case null => write[ChunkListNode[V, W]](null)
          case node => node.map(f, memo)
        }
      }, conf, workerId
    )
  }

  def merge(that: ChunkList[T, U], comparator: ((T, U), (T, U)) => Int)
      (implicit c: Context): ChunkList[T, U] = {
    merge(that, new Memoizer[Changeable[ChunkListNode[T, U]]](), comparator)
  }

  def merge
      (that: ChunkList[T, U],
       memo: Memoizer[Changeable[ChunkListNode[T, U]]],
       comparator: ((T, U), (T, U)) => Int)
      (implicit c: Context): ChunkList[T, U] = {

    def innerMerge
        (one: ChunkListNode[T, U],
         two: ChunkListNode[T, U],
         _oneR: Vector[(T, U)],
         _twoR: Vector[(T, U)],
         memo: Memoizer[Changeable[ChunkListNode[T, U]]])
        (implicit c: Context): Changeable[ChunkListNode[T, U]] = {
      val oneR =
	if (one == null)
	  _oneR
	else
	  _oneR ++ one.chunk

      val twoR =
	if (two == null)
	  _twoR
	else
	  _twoR ++ two.chunk

      var i = 0
      var j = 0
      val newChunk =
	if (oneR.size == 0) {
	  j = twoR.size
	  twoR
	} else if (twoR.size == 0) {
	  i = oneR.size
	  oneR
	} else {
	  val buf = Buffer[(T, U)]()
	  while (i < oneR.size && j < twoR.size) {
	    if (comparator(oneR(i), twoR(j)) < 0) {
	      buf += oneR(i)
	      i += 1
	    } else {
	      buf += twoR(j)
	      j += 1
	    }
	  }

	  buf.toVector
	}

      val newOneR = oneR.drop(i)
      val newTwoR = twoR.drop(j)

      val newNext = mod {
	if (one == null) {
	  if (two == null) {
            val rest = newOneR ++ newTwoR
            if (rest.size > 0) {
              val newTail = mod[ChunkListNode[T, U]]{ write(null) }
	      write(new ChunkListNode(rest, newTail))
            } else {
              write[ChunkListNode[T, U]](null)
            }
	  } else {
	    read(two.nextMod) {
	      case twoNode =>
		memo(null, twoNode, newOneR, newTwoR) {
		  innerMerge(null, twoNode, newOneR, newTwoR, memo)
		}
	    }
	  }
	} else {
	  read(one.nextMod) {
	    case oneNode =>
	      if (two == null) {
		memo(oneNode, null, newOneR, newTwoR) {
		  innerMerge(oneNode, null, newOneR, newTwoR, memo)
		}
	      } else {
		read(two.nextMod) {
		  case twoNode =>
		    memo(oneNode, twoNode, newOneR, newTwoR) {
		      innerMerge(oneNode, twoNode, newOneR, newTwoR, memo)
		    }
		}
	      }
	  }
	}
      }

      if (newChunk.size == 0) {
        write[ChunkListNode[T, U]](null)
      } else {
        write(new ChunkListNode(newChunk, newNext))
      }
    }

    new ChunkList(
      mod {
	read(head) {
	  case null => read(that.head) { write(_) }
	  case node =>
	    read(that.head) {
	      case null => write(node)
	      case thatNode =>
                val empty = Vector[(T, U)]()
		memo(node, thatNode, empty, empty) {
		  innerMerge(node, thatNode, empty, empty, memo)
		}
	    }
	}
      }, conf
    )
  }

  override def mergesort(cmp: ((T, U), (T, U)) => Int)
      (implicit c: Context): ChunkList[T, U] = {
    def comparator(pair1: (T, U), pair2: (T, U)) = {
      cmp(pair1, pair2) < 0
    }

    val modizer = new Modizer1[ChunkListNode[T, U]]()
    def mapper(chunk: Vector[(T, U)], key: ModId) = {
      val tail = modizer(key) {
	write(new ChunkListNode[T, U]((chunk.toBuffer.sortWith(comparator).toVector), mod({ write(null) })))
      }

      ("", new ChunkList(tail, conf))
    }

    val memo = new Memoizer[ChunkList[T, U]]()

    def reducer(pair1: (String, ChunkList[T, U]), pair2: (String, ChunkList[T, U])) = {
      val merged = memo(pair1._2, pair2._2) {
        val memoizer = new Memoizer[Changeable[ChunkListNode[T, U]]]()

	pair2._2.merge(pair1._2, memoizer, cmp)
      }

      (pair1._1 + pair2._1, merged)
    }

    val mapped = keyedChunkMap(mapper)
    val reduced = mapped.reduce(reducer)

    new ChunkList(
      mod {
        read(reduced) {
	  case null => write(null)
          case (key, list) => read(list.head) { write(_) }
        }
      }, conf
    )
  }

  def reduce(f: ((T, U), (T, U)) => (T, U))
      (implicit c: Context): Mod[(T, U)] = ???

  override def reduceByKey(f: (U, U) => U, comparator: ((T, U), (T, U)) => Int)
      (implicit c: Context): ChunkList[T, U] = {
    val sorted = this.mergesort( comparator)

    new ChunkList(
      mod {
	read(sorted.head) {
	  case null =>
	    write(null)
	  case node =>
	    node.reduceByKey(f, node.chunk.head._1, null.asInstanceOf[U])
	}
      }, conf
    )
  }

  def sortJoin[V](that: AdjustableList[T, V])
      (implicit c: Context, ordering: Ordering[T]): AdjustableList[T, (U, V)] = ???

  def split(pred: ((T, U)) => Boolean)
      (implicit c: Context): (ChunkList[T, U], ChunkList[T, U]) = ???

  /* Meta functions */
  def toBuffer(mutator: Mutator): Buffer[(T, U)] = {
    val buf = Buffer[(T, U)]()
    if (head != null) {
      var node = mutator.read(head)
      while (node != null) {
	buf ++= node.chunk
	node = mutator.read(node.nextMod)
      }
    }

    buf
  }

  override def equals(that: Any): Boolean = {
    that match {
      case thatList: ChunkList[T, U] => head == thatList.head
      case _ => false
    }
  }

  override def toString = "ChunkList[" + head.toString + "]"
}
