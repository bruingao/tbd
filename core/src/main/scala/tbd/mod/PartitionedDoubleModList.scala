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

import akka.actor.ActorRef
import scala.collection.mutable.{ArrayBuffer, Buffer, Set}

import tbd.{Changeable, TBD}
import tbd.datastore.Datastore

class PartitionedDoubleModList[T](
    aPartitions: ArrayBuffer[DoubleModList[T]]) extends AdjustableList[T] {
  val partitions = aPartitions

  def map[U](
      tbd: TBD,
      f: (TBD, T) => U,
      parallel: Boolean = true,
      memoized: Boolean = false): PartitionedDoubleModList[U] = {
    if (parallel) {
      def innerMemoParMap(tbd: TBD, i: Int): ArrayBuffer[DoubleModList[U]] = {
        if (i < partitions.size) {
          val parTup = tbd.par((tbd: TBD) => {
            partitions(i).map(tbd, f, memoized = memoized)
          }, (tbd: TBD) => {
            innerMemoParMap(tbd, i + 1)
          })

          parTup._2 += parTup._1
        } else {
          ArrayBuffer[DoubleModList[U]]()
        }
      }

      new PartitionedDoubleModList(innerMemoParMap(tbd, 0))
    } else {
      new PartitionedDoubleModList(
        partitions.map((partition: DoubleModList[T]) => {
          partition.map(tbd, f, memoized = memoized)
        })
      )
    }
  }
  
  /*
  * Note: Placeholder to satisfy the trait implementation
  */
  def foldl(tbd: TBD, initialValueMod: Mod[T], f: (TBD, T, T) => T) : Mod[T] = {
    throw new java.lang.UnsupportedOperationException("Not Implemented")
    tbd.mod((dest : Dest[T]) => 
    tbd.read(initialValueMod, (initialValue: T) =>
    tbd.write(dest, initialValue))) 
  }

  def reduce(tbd: TBD, initialValueMod: Mod[T], f: (TBD, T, T) => T) : Mod[T] = {
    foldl(tbd, initialValueMod, f)
  }

  def filter(
      tbd: TBD,
      pred: T => Boolean,
      parallel: Boolean = true,
      memoized: Boolean = true): PartitionedDoubleModList[T] = {
    def parFilter(tbd: TBD, i: Int): ArrayBuffer[DoubleModList[T]] = {
      if (i < partitions.size) {
        val parTup = tbd.par((tbd: TBD) => {
          partitions(i).filter(tbd, pred, parallel, memoized)
        }, (tbd: TBD) => {
          parFilter(tbd, i + 1)
        })

        parTup._2 += parTup._1
      } else {
        ArrayBuffer[DoubleModList[T]]()
      }
    }

    if (parallel) {
      new PartitionedDoubleModList(parFilter(tbd, 0))
    } else {
      new PartitionedDoubleModList(
        partitions.map((partition: DoubleModList[T]) => {
          partition.filter(tbd, pred, parallel, memoized)
        })
      )
    }
  }

  /* Meta Operations */
  def toBuffer(): Buffer[T] = {
    val buf = ArrayBuffer[T]()

    for (partition <- partitions) {
      var innerNode = partition.head.read()
      while (innerNode != null) {
        buf += innerNode.valueMod.read()
        innerNode = innerNode.next.read()
      }
    }

    buf
  }
}
