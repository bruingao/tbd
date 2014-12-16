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
import tbd.TBD._

class PartitionedChunkList[T, U]
    (val partitions: Buffer[ChunkList[T, U]],
     conf: ListConf) extends AdjustableList[T, U] with Serializable {

  override def chunkMap[V, W](f: (Vector[(T, U)]) => (V, W))
      (implicit c: Context): PartitionedModList[V, W] = {
    def innerChunkMap(i: Int)(implicit c: Context): Buffer[ModList[V, W]] = {
      if (i < partitions.size) {
        val (mappedPartition, mappedRest) = parWithHint({
          c => partitions(i).chunkMap(f)(c)
        }, partitions(i).workerId)({
          c => innerChunkMap(i + 1)(c)
        })

        mappedRest += mappedPartition
      } else {
        Buffer[ModList[V, W]]()
      }
    }

    new PartitionedModList(innerChunkMap(0))
  }

  def filter(pred: ((T, U)) => Boolean)
      (implicit c: Context): PartitionedChunkList[T, U] = ???

  def flatMap[V, W](f: ((T, U)) => Iterable[(V, W)])
      (implicit c: Context): PartitionedChunkList[V, W] = {
    def innerMap(i: Int)(implicit c: Context): Buffer[ChunkList[V, W]] = {
      if (i < partitions.size) {
        val (mappedPartition, mappedRest) = parWithHint({
          c => partitions(i).flatMap(f)(c)
        }, partitions(i).workerId)({
          c => innerMap(i + 1)(c)
        })

        mappedRest += mappedPartition
      } else {
        Buffer[ChunkList[V, W]]()
      }
    }

    new PartitionedChunkList(innerMap(0), conf)
  }

  def join[V](that: AdjustableList[T, V], condition: ((T, V), (T, U)) => Boolean)
      (implicit c: Context): PartitionedChunkList[T, (U, V)] = ???

  def map[V, W](f: ((T, U)) => (V, W))
      (implicit c: Context): PartitionedChunkList[V, W] = {
    def innerMap(i: Int)(implicit c: Context): Buffer[ChunkList[V, W]] = {
      if (i < partitions.size) {
        val (mappedPartition, mappedRest) = parWithHint({
          c => partitions(i).map(f)(c)
        }, partitions(i).workerId)({
          c => innerMap(i + 1)(c)
        })

        mappedRest += mappedPartition
      } else {
        Buffer[ChunkList[V, W]]()
      }
    }

    new PartitionedChunkList(innerMap(0), conf)
  }

  override def mergesort(comparator: ((T, U), (T, U)) => Int)
      (implicit c: Context): PartitionedChunkList[T, U] = {
    def innerSort(i: Int)(implicit c: Context): ChunkList[T, U] = {
      if (i < partitions.size) {
        val (sortedPartition, sortedRest) = par {
          c => partitions(i).mergesort(comparator)(c)
        } and {
          c => innerSort(i + 1)(c)
        }

	sortedPartition.merge(sortedRest, comparator)
      } else {
	new ChunkList[T, U](mod { write[ChunkListNode[T, U]](null) }, conf)
      }
    }

    // TODO: make the output have as many partitions as this list.
    new PartitionedChunkList[T, U](Buffer(innerSort(0)), conf)
  }

  def reduce(f: ((T, U), (T, U)) => (T, U))
      (implicit c: Context): Mod[(T, U)] = ???

  def sortJoin[V](that: AdjustableList[T, V])
      (implicit c: Context,
       ordering: Ordering[T]): AdjustableList[T, (U, V)] = ???

  def split(pred: ((T, U)) => Boolean)
      (implicit c: Context):
    (PartitionedChunkList[T, U], PartitionedChunkList[T, U]) = ???

  /* Meta Operations */
  def toBuffer(mutator: Mutator): Buffer[(T, U)] = {
    val buf = Buffer[(T, U)]()

    for (partition <- partitions) {
      buf ++= partition.toBuffer(mutator)
    }

    buf
  }
}
