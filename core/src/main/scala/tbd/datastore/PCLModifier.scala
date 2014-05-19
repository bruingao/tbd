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
package tbd.datastore

import akka.actor.ActorRef
import scala.collection.mutable.{ArrayBuffer, Map}

import tbd.mod._

class PCLModifier[T, U](
    _datastore: Datastore,
    table: Map[Any, Any],
    numPartitions: Int,
    chunkSize: Int,
    chunkSizer: U => Int) extends Modifier[T, U](_datastore) {

  val partitionModifiers = ArrayBuffer[ChunkListModifier[T, U]]()
  val list = initialize()

  private def initialize(): PartitionedChunkList[T, U] = {
    val partitions = new ArrayBuffer[ChunkList[T, U]]()
    var chunkListModifier =
      new ChunkListModifier[T, U](datastore, Map[Any, Any](), chunkSize, chunkSizer)

    val partitionSize = math.max(1, table.size / numPartitions)
    var i = 1
    for (elem <- table) {
      chunkListModifier.insert(elem._1.asInstanceOf[T],
			       elem._2.asInstanceOf[U], null)

      if (i % partitionSize == 0) {
        partitionModifiers += chunkListModifier
        partitions += chunkListModifier.list
        chunkListModifier = new ChunkListModifier[T, U](datastore,
							Map[Any, Any](),
							chunkSize,
							chunkSizer)
      }
      i += 1
    }

    if ((i - 1) % partitionSize != 0) {
      partitionModifiers += chunkListModifier
      partitions += chunkListModifier.list
    }

    new PartitionedChunkList[T, U](partitions)
  }

  def insert(key: T, value: U, respondTo: ActorRef): Int = {
    partitionModifiers(0).insert(key, value, respondTo)
  }

  def update(key: T, value: U, respondTo: ActorRef): Int = {
    var count = 0

    var found = false
    for (partitionModifier <- partitionModifiers) {
      if (!found && partitionModifier.contains(key)) {
        count = partitionModifier.update(key, value, respondTo)
        found = true
      }
    }

   if (!found) {
     println("Warning: tried to update nonexistant key " + key)
   }

    count
  }

  def remove(key: T, respondTo: ActorRef): Int = {
    var count = 0

    var found = false
    for (partitionModifier <- partitionModifiers) {
      if (!found && partitionModifier.contains(key)) {
        count = partitionModifier.remove(key, respondTo)
        found = true
      }
    }

   if (!found) {
     println("Warning: tried to remove nonexistant key " + key)
   }

    count
  }

  def getModifiable(): AdjustableList[T, U] = list
}
