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

class PartitionedModList[T, U]
    (val partitions: Buffer[ModList[T, U]])
  extends AdjustableList[T, U] with Serializable {

  def filter(pred: ((T, U)) => Boolean)
      (implicit c: Context): PartitionedModList[T, U] = {
    def parFilter(i: Int)(implicit c: Context): Buffer[ModList[T, U]] = {
      if (i < partitions.size) {
        val (filteredPartition, filteredRest) = parWithHint({
          c => partitions(i).filter(pred)(c)
        }, partitions(i).workerId)({
          c => parFilter(i + 1)(c)
        })

        filteredRest += filteredPartition
      } else {
        Buffer[ModList[T, U]]()
      }
    }

    new PartitionedModList(parFilter(0))
  }

  def flatMap[V, W](f: ((T, U)) => Iterable[(V, W)])
      (implicit c: Context): PartitionedModList[V, W] = {
    def innerFlatMap(i: Int)(implicit c: Context): Buffer[ModList[V, W]] = {
      if (i < partitions.size) {
        val (mappedPartition, mappedRest) = parWithHint({
          c => partitions(i).flatMap(f)(c)
        }, partitions(i).workerId)({
          c => innerFlatMap(i + 1)(c)
        })

        mappedRest += mappedPartition
      } else {
        Buffer[ModList[V, W]]()
      }
    }

    new PartitionedModList(innerFlatMap(0))
  }

  def join[V](that: AdjustableList[T, V], condition: ((T, V), (T, U)) => Boolean)
      (implicit c: Context): PartitionedModList[T, (U, V)] = {
    def innerJoin(i: Int)(implicit c: Context): Buffer[ModList[T, (U, V)]] = {
      if (i < partitions.size) {
	val (joinedPartition, joinedRest) = par {
	  c => partitions(i).join(that, condition)(c)
	} and {
	  c => innerJoin(i + 1)(c)
	}

	joinedRest += joinedPartition
      } else {
	Buffer[ModList[T, (U, V)]]()
      }
    }

    new PartitionedModList(innerJoin(0))
  }

  def map[V, W](f: ((T, U)) => (V, W))
      (implicit c: Context): PartitionedModList[V, W] = {
    def innerMap(i: Int)(implicit c: Context): Buffer[ModList[V, W]] = {
      if (i < partitions.size) {
        val (mappedPartition, mappedRest) = parWithHint({
          c => partitions(i).map(f)(c)
        }, partitions(i).workerId)({
          c => innerMap(i + 1)(c)
        })

        mappedRest += mappedPartition
      } else {
        Buffer[ModList[V, W]]()
      }
    }

    new PartitionedModList(innerMap(0))
  }

  override def mergesort(comparator: ((T, U), (T, U)) => Int)
      (implicit c: Context): PartitionedModList[T, U] = {
    def innerSort(i: Int)(implicit c: Context): ModList[T, U] = {
      if (i < partitions.size) {
        val (sortedPartition, sortedRest) = par {
          c => partitions(i).mergesort(comparator)(c)
        } and {
          c => innerSort(i + 1)(c)
        }

	sortedPartition.merge(sortedRest, comparator)
      } else {
        new ModList[T, U](mod { write[ModListNode[T, U]](null) })
      }
    }

    // TODO: make the output have as many partitions as this list.
    new PartitionedModList(Buffer(innerSort(0)))
  }

  override def quicksort(comparator: ((T, U), (T, U) ) => Int)
      (implicit c: Context): PartitionedModList[T, U] = {
    def innerSort(i: Int)(implicit c: Context): ModList[T, U] = {
      if (i < partitions.size) {
        val (sortedPartition, sortedRest) = par {
          c => partitions(i).quicksort(comparator)(c)
        } and {
          c => innerSort(i + 1)(c)
        }
        /*
        val comp = (pair1: (T, _), pair2: (T, _)) => {
          comparator(pair1, pair2)
	      //ordering.compare(pair1._1, pair2._1)
	    }
		*/
	sortedPartition.merge(sortedRest, comparator)
      } else {
        new ModList[T, U](mod { write[ModListNode[T, U]](null) })
      }
    }

    // TODO: make the output have as many partitions as this list.
    new PartitionedModList(Buffer(innerSort(0)))
  }

  def reduce(f: ((T, U), (T, U)) => (T, U))
      (implicit c: Context): Mod[(T, U)] = {

    def innerReduce
        (next: ModList[T, U],
         remaining: Buffer[ModList[T, U]])
        (implicit c: Context): Mod[(T, U)] = {
      val newNextOption = remaining.find(_.workerId == next.workerId)

      val (reducedPartition, reducedRest) =
        newNextOption match {
          case Some(newNext) =>
            parWithHint({
              c => next.reduce(f)(c)
            }, next.workerId)({
              c => innerReduce(newNext, remaining - newNext)(c)
            }, newNext.workerId)
        case None =>
          if (remaining.size > 0) {
            parWithHint({
              c => next.reduce(f)(c)
            }, next.workerId)({
              c => innerReduce(remaining(0), remaining.tail)(c)
            }, remaining(0).workerId)
          } else {
            parWithHint({
              c => next.reduce(f)(c)
            }, next.workerId)({
              c => mod { write[(T, U)](null)(c) }(c)
            }, next.workerId)
          }
        }

      mod {
        read(reducedPartition) {
          case null =>
            read(reducedRest) {
              case null => write(null)
              case rest => write(rest)
            }
          case partition =>
            read(reducedRest) {
              case null => write(partition)
              case rest => write(f(partition, rest))
            }
        }
      }
    }

    parWithHint({
      c => innerReduce(partitions(0), partitions.tail)(c)
    }, partitions(0).workerId)({
      c =>
    })._1
  }

  def sortJoin[V](that: AdjustableList[T, V])
      (implicit c: Context,
       ordering: Ordering[T]): AdjustableList[T, (U, V)] = ???

  def split(pred: ((T, U)) => Boolean)
      (implicit c: Context):
    (PartitionedModList[T, U], PartitionedModList[T, U]) = ???

  /* Meta Operations */
  def toBuffer(mutator: Mutator): Buffer[(T, U)] = {
    val buf = Buffer[(T, U)]()

    for (partition <- partitions) {
      buf ++= partition.toBuffer(mutator)
    }

    buf
  }
}
