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

import tbd.{Changeable, Context}
import tbd.datastore.Datastore
import tbd.TBD._

class PartitionedModList[T, U](
    val partitions: ArrayBuffer[ModList[T, U]]
  ) extends AdjustableList[T, U] {

  def filter(
      pred: ((T, U)) => Boolean)
     (implicit c: Context): PartitionedModList[T, U] = {
    def parFilter(i: Int)(implicit c: Context): ArrayBuffer[ModList[T, U]] = {
      if (i < partitions.size) {
        val parTup = par {
          c => partitions(i).filter(pred)(c)
        } and {
          c => parFilter(i + 1)(c)
        }

        parTup._2 += parTup._1
      } else {
        ArrayBuffer[ModList[T, U]]()
      }
    }

    new PartitionedModList(parFilter(0))
  }

  def map[V, W](
      f: ((T, U)) => (V, W))
     (implicit c: Context): PartitionedModList[V, W] = {
    def innerMap(i: Int)(implicit c: Context): ArrayBuffer[ModList[V, W]] = {
      if (i < partitions.size) {
        val parTup = par {
          c => partitions(i).map(f)(c)
        } and {
          c => innerMap(i + 1)(c)
        }

        parTup._2 += parTup._1
      } else {
        ArrayBuffer[ModList[V, W]]()
      }
    }

    new PartitionedModList(innerMap(0))
  }

  def reduce(
      initialValueMod: Mod[(T, U)],
      f: ((T, U), (T, U)) => (T, U))
     (implicit c: Context): Mod[(T, U)] = {

    def parReduce(i: Int)(implicit c: Context): Mod[(T, U)] = {
      if (i < partitions.size) {
        val parTup = par {
          c => partitions(i).reduce(initialValueMod, f)(c)
        } and {
          c => parReduce(i + 1)(c)
        }

        mod {
          read2(parTup._1, parTup._2) {
	    case (a, b) => write(f(a, b))
          }
        }
      } else {
        initialValueMod
      }
    }

    parReduce(0)
  }

  def sort(
      comparator: ((T, U), (T, U)) => Boolean)
     (implicit c: Context): AdjustableList[T, U] = ???

  def split(
      pred: ((T, U)) => Boolean)
     (implicit c: Context): (AdjustableList[T, U], AdjustableList[T, U]) = ???

  /* Meta Operations */
  def toBuffer(): Buffer[U] = {
    val buf = ArrayBuffer[U]()

    for (partition <- partitions) {
      var innerNode = partition.head.read()
      while (innerNode != null) {
        buf += innerNode.value._2
        innerNode = innerNode.next.read()
      }
    }

    buf
  }
}
