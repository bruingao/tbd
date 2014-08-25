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

import tbd._
import tbd.TBD._

// The default value of zero for size works because size is only ever
// accessed by the Modifier, which will set it appropriately.
class ChunkListNode[T, U](
    val chunk: Vector[(T, U)],
    val nextMod: Mod[ChunkListNode[T, U]],
    val size: Int = 0) extends Serializable {

  override def equals(obj: Any): Boolean = {
    if (!obj.isInstanceOf[ChunkListNode[T, U]]) {
      false
    } else {
      val that = obj.asInstanceOf[ChunkListNode[T, U]]
      that.chunk == chunk && that.nextMod == nextMod
    }
  }

  def chunkMap[V, W](
      f: (Vector[(T, U)]) => (V, W),
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

  def loopJoin[V](
      that: ChunkList[T, V],
      comparator: ((T, U), (T, V)) => Boolean,
      memo: Memoizer[Changeable[ChunkListNode[T, (U, V)]]])
     (implicit c: Context): Changeable[ChunkListNode[T, (U, V)]] = {
    val newNext = mod {
      read(nextMod) {
	case null =>
	  write[ChunkListNode[T, (U, V)]](null)
	case node =>
	  memo(node) {
	    node.loopJoin(that, comparator, memo)
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
	    val memo2 = makeMemoizer[Changeable[ChunkListNode[T, (U, V)]]]()
	    node.joinHelper(chunk(i), comparator, tail, memo2)
	  }
	}

	val memo2 = makeMemoizer[Changeable[ChunkListNode[T, (U, V)]]]()
	node.joinHelper(chunk(chunk.size - 1), comparator, tail, memo2)
    }
  }

  // Iterates over the second join list, testing each element for equality
  // with a single element from the first list.
  private def joinHelper[V](
      thatValue: (T, V),
      comparator: ((T, V), (T, U)) => Boolean,
      tail: Mod[ChunkListNode[T, (V, U)]],
      memo: Memoizer[Changeable[ChunkListNode[T, (V, U)]]])
     (implicit c: Context): Changeable[ChunkListNode[T, (V, U)]] = {
    var newChunk = Vector[(T, (V, U))]()
    for (value <- chunk) {
      if (comparator(thatValue, value)) {
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
	      node.joinHelper(thatValue, comparator, tail, memo)
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
	    node.joinHelper(thatValue, comparator, tail, memo)
	  }
      }
    }
  }

  def map[V, W](
      f: ((T, U)) => (V, W),
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

  override def toString = "Node(" + chunk + ", " + nextMod + ")"
}