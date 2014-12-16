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
import tbd.Constants.ModId
import tbd.TBD._

// The default value of zero for size works because size is only ever
// accessed by the Modifier, which will set it appropriately.
class ChunkListNode[T, U]
    (val chunk: Vector[(T, U)],
     val nextMod: Mod[ChunkListNode[T, U]],
     val size: Int = 0) extends Serializable {

  def chunkMap[V, W]
      (f: (Vector[(T, U)]) => (V, W),
       memo: Memoizer[Mod[ModListNode[V, W]]])
      (implicit c: Context): Changeable[ModListNode[V, W]] = {
    val newNextMod = memo(nextMod) {
      mod {
        read(nextMod)(next => {
          if (next != null && next.chunk.size > 0)
            next.chunkMap(f, memo)
          else
            write[ModListNode[V, W]](null)
        })
      }
    }

    write(new ModListNode[V, W](f(chunk), newNextMod))
  }

  def flatMap[V, W]
      (f: ((T, U)) => Iterable[(V, W)],
       chunkSize: Int,
       memo: Memoizer[Changeable[ChunkListNode[V, W]]])
      (implicit c: Context): Changeable[ChunkListNode[V, W]] = {
    var tail = mod {
      read(nextMod) {
	case null => write[ChunkListNode[V, W]](null)
	case next =>
          memo(nextMod) {
            next.flatMap(f, chunkSize, memo)
          }
      }
    }

    var mapped = chunk.flatMap(f)

    while (mapped.size > chunkSize) {
      val (start, end) = mapped.splitAt(chunkSize)
      tail = mod {
	write(new ChunkListNode[V, W](start, tail))
      }
      mapped = end
    }

    write(new ChunkListNode[V, W](mapped, tail))
  }

  def loopJoin[V]
      (that: ChunkList[T, V],
       memo: Memoizer[Changeable[ChunkListNode[T, (U, V)]]],
       condition: ((T, V), (T, U)) => Boolean)
      (implicit c: Context): Changeable[ChunkListNode[T, (U, V)]] = {
    val newNext = mod {
      read(nextMod) {
	case null =>
	  write[ChunkListNode[T, (U, V)]](null)
	case node =>
	  memo(node) {
	    node.loopJoin(that, memo, condition)
	  }
      }
    }

    read(that.head) {
      case null =>
	read(newNext) { write(_) }
      case node =>
	var tail = newNext
	for (i <- 0 until chunk.size - 1) {
	  tail = mod {
	    val memo2 = new Memoizer[Changeable[ChunkListNode[T, (U, V)]]]()
	    node.joinHelper(chunk(i),  tail, memo2, condition)
	  }
	}

	val memo2 = new Memoizer[Changeable[ChunkListNode[T, (U, V)]]]()
	node.joinHelper(chunk(chunk.size - 1), tail, memo2, condition)
    }
  }

  // Iterates over the second join list, testing each element for equality
  // with a single element from the first list.
  private def joinHelper[V]
      (thatValue: (T, V),
       tail: Mod[ChunkListNode[T, (V, U)]],
       memo: Memoizer[Changeable[ChunkListNode[T, (V, U)]]],
       condition: ((T, U), (T, V)) => Boolean)
      (implicit c: Context): Changeable[ChunkListNode[T, (V, U)]] = {
    var newChunk = Vector[(T, (V, U))]()
    for (value <- chunk) {
      if (condition(value, thatValue)) {
	val newValue = (value._1, (thatValue._2, value._2))
	newChunk :+= newValue
      }
    }

    if (newChunk.size > 0) {
      read(nextMod) {
	case null =>
	  write(new ChunkListNode[T, (V, U)](newChunk, tail))
	case node =>
	  val newNext = mod {
	    memo(node) {
	      node.joinHelper(thatValue, tail, memo, condition)
	    }
	  }

	  write(new ChunkListNode[T, (V, U)](newChunk, newNext))
      }
    } else {
      read(nextMod) {
	case null =>
	  read(tail) { write(_) }
	case node =>
	  memo(node) {
	    node.joinHelper(thatValue, tail, memo, condition)
	  }
      }
    }
  }

  def keyedChunkMap[V, W]
      (f: (Vector[(T, U)], ModId) => (V, W),
       memo: Memoizer[Mod[ModListNode[V, W]]],
       thisId: ModId)
      (implicit c: Context): Changeable[ModListNode[V, W]] = {
    val newNextMod = memo(nextMod) {
      mod {
        read(nextMod)(next => {
          if (next != null && next.chunk.size > 0)
            next.keyedChunkMap(f, memo, nextMod.id)
          else
            write[ModListNode[V, W]](null)
        })
      }
    }

    write(new ModListNode[V, W](f(chunk, thisId), newNextMod))
  }

  def map[V, W]
      (f: ((T, U)) => (V, W),
       memo: Memoizer[Changeable[ChunkListNode[V, W]]])
      (implicit c: Context): Changeable[ChunkListNode[V, W]] = {
    val newChunk = chunk.map(f)
    val newNext = mod {
      read(nextMod) {
	case null => write[ChunkListNode[V, W]](null)
	case next =>
          memo(nextMod) {
            next.map(f, memo)
          }
      }
    }

    write(new ChunkListNode[V, W](newChunk, newNext))
  }

  def reduceByKey
      (f: (U, U) => U,
       previousKey: T,
       runningValue: U)
      (implicit c: Context): Changeable[ChunkListNode[T, U]] = {
    var remaining = chunk
    var reduced = Buffer[(T, U)]()

    var newRunningValue = runningValue
    var newPreviousKey = previousKey

    while (remaining.size > 0) {
      while (remaining.size > 0 && remaining.head._1 == newPreviousKey) {
	newRunningValue =
	  if (newRunningValue == null)
	    remaining.head._2
	  else
	    f(newRunningValue, remaining.head._2)

	remaining = remaining.tail
      }

      if (remaining.size > 0) {
	reduced += ((newPreviousKey, newRunningValue))
	newPreviousKey = remaining.head._1
	newRunningValue = null.asInstanceOf[U]
      }
    }

    if (reduced.size > 0) {
      var tail = mod {
	read(nextMod) {
	  case null =>
	    val tail = mod { write[ChunkListNode[T, U]](null) }
	    write(new ChunkListNode(Vector((newPreviousKey, newRunningValue)), tail))
	  case node =>
	    node.reduceByKey(f, newPreviousKey, newRunningValue)
	}
      }

      while (reduced.size > 1) {
	tail = mod {
	  write(new ChunkListNode(Vector(reduced.head), tail))
	}
	reduced = reduced.tail
      }

      write(new ChunkListNode(Vector(reduced.head), tail))
    } else {
      read(nextMod) {
	case null =>
	  val tail = mod { write[ChunkListNode[T, U]](null) }
	  write(new ChunkListNode(Vector((newPreviousKey, newRunningValue)), tail))
	case node =>
	  node.reduceByKey(f, newPreviousKey, newRunningValue)
      }
    }

    /*if (value._1 == previousKey) {
      val newRunningValue =
	if (runningValue == null)
	  value._2
	else
	  f(runningValue, value._2)

      read(nextMod) {
	case null =>
	  val tail = mod { write[ChunkListNode[T, U]](null) }
	  write(new ChunkListNode((value._1, newRunningValue), tail))
	case node =>
	  node.reduceByKey(f, value._1, newRunningValue)
      }
    } else {
      val newNextMod = mod {
	read(nextMod) {
	  case null =>
	    val tail = mod { write[ChunkListNode[T, U]](null) }
	    write(new ChunkListNode(value, tail))
	  case node =>
	    node.reduceByKey(f, value._1, value._2)
	}
      }

      write(new ChunkListNode((previousKey, runningValue), newNextMod))
    }*/
  }

  override def equals(obj: Any): Boolean = {
    if (!obj.isInstanceOf[ChunkListNode[T, U]]) {
      false
    } else {
      val that = obj.asInstanceOf[ChunkListNode[T, U]]
      that.chunk == chunk && that.nextMod == nextMod
    }
  }

  override def toString = "Node(" + chunk + ", " + nextMod + ")"
}
