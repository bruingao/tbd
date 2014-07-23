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

import scala.collection.mutable.Buffer

import tbd.{Changeable, Context, Memoizer}
import tbd.Constants.ModId
import tbd.TBD._

class ChunkList[T, U](
    val head: Mod[ChunkListNode[T, U]]) extends AdjustableChunkList[T, U] {

  def map[V, W](
      f: ((T, U)) => (V, W))
     (implicit c: Context): ChunkList[V, W] = {
    val memo = makeMemoizer[Changeable[ChunkListNode[V, W]]]()
    new ChunkList(
      mod {
        read(head) {
	  case null => write[ChunkListNode[V, W]](null)
	  case node => node.map(f, memo)
        }
      }
    )
  }

  def chunkMap[V, Q](
      f: (Vector[(T, U)]) => (V, Q))
     (implicit c: Context): ModList[V, Q] = {
    val memo = makeMemoizer[Mod[ModListNode[V, Q]]]()
    new ModList(
      mod {
        read(head) {
	  case null => write[ModListNode[V, Q]](null)
	  case node => node.chunkMap(f, memo)
        }
      }
    )
  }

  def filter(
      pred: ((T, U)) => Boolean)
     (implicit c: Context): ChunkList[T, U] = ???

  def reduce(
      initialValueMod: Mod[(T, U)],
      f: ((T, U), (T, U)) => (T, U))
     (implicit c: Context): Mod[(T, U)] = ???

  def split(
      pred: ((T, U)) => Boolean)
     (implicit c: Context): (AdjustableList[T, U], AdjustableList[T, U]) = ???

  def sort(
      comparator: ((T, U), (T, U)) => Boolean)
     (implicit c: Context): AdjustableList[T, U] = ???

  def chunkSort(
      comparator: ((T, U), (T, U)) => Boolean)
     (implicit c: Context): Mod[(Int, Vector[(T, U)])] = {
    def mapper(chunk: Vector[(T, U)]): (Int, Vector[(T, U)]) = {
      (0, chunk.sortWith((pair1: (T, U), pair2: (T, U)) => {
	comparator(pair1, pair2)
      }))
    }
    val sortedChunks = chunkMap(mapper)

    def reducer(pair1: (Int, Vector[(T, U)]), pair2: (Int, Vector[(T, U)])) = {
      def innerReducer(v1: Vector[(T, U)], v2: Vector[(T, U)]): Vector[(T, U)] = {
	if (v1.size == 0) {
	  v2
	} else if (v2.size == 0) {
	  v1
	} else {
	  if (comparator(v1.head, v2.head)) {
	    v1.head +: innerReducer(v1.tail, v2)
	  } else {
	    v2.head +: innerReducer(v1, v2.tail)
	  }
	}
      }
      (pair1._1, innerReducer(pair1._2, pair2._2))
    }

    val initialValue = createMod((0, Vector[(T, U)]()))
    sortedChunks.reduce(initialValue, reducer)
  }

  /* Meta functions */
  def toBuffer(): Buffer[U] = {
    val buf = Buffer[U]()
    var node = head.read()
    while (node != null) {
      buf ++= node.chunk.map(value => value._2)
      node = node.nextMod.read()
    }

    buf
  }
}
