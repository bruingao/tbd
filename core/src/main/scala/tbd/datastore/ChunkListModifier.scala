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
import scala.collection.mutable.Map

import tbd.mod.{ChunkList, ChunkListNode, Mod}

class ChunkListModifier[T, U](
    _datastore: Datastore,
    table: Map[Any, Any],
    chunkSize: Int,
    chunkSizer: U => Int) extends Modifier[T, U](_datastore) {
  private var tailMod: Mod[ChunkListNode[T, U]] = null

  // Contains the last ChunkListNode before the tail node.
  private var lastNodeMod: Mod[ChunkListNode[T, U]] = null

  val list = initialize()

  private def initialize(): ChunkList[T, U] = {
    var tail = datastore.createMod[ChunkListNode[T, U]](null)
    tailMod = tail

    val lastChunkMod = datastore.createMod(Vector[(T, U)]())
    tail = datastore.createMod(new ChunkListNode(lastChunkMod, tail, 0))
    lastNodeMod = tail

    var chunk = Vector[(T, U)]()
    var size = 0
    for (elem <- table) {
      if (size >= chunkSize) {
	val chunkMod = datastore.createMod(chunk)
	tail = datastore.createMod(new ChunkListNode(chunkMod, tail, size))

	chunk = Vector[(T, U)]()
	size = 0
      }

      chunk = chunk :+ (elem._1.asInstanceOf[T] -> elem._2.asInstanceOf[U])
      size += chunkSizer(elem._2.asInstanceOf[U])
    }

    if (size > 0) {
      val chunkMod = datastore.createMod(chunk)
      tail = datastore.createMod(new ChunkListNode(chunkMod, tail, size))
    }

    new ChunkList[T, U](tail)
  }

  def insert(key: T, value: U, respondTo: ActorRef): Int = {
    val lastNode =
      datastore.getMod(lastNodeMod.id).asInstanceOf[ChunkListNode[T, U]]

    var count = 0
    if (lastNode == null) {
      val chunkMod = datastore.createMod(Vector[(T, U)]((key -> value)))
      val size = chunkSizer(value)

      val newNode = new ChunkListNode(chunkMod, tailMod, size)
      count += datastore.updateMod(lastNodeMod.id, newNode, respondTo)
    } else if (lastNode.size >= chunkSize) {
      val lastChunk = datastore.getMod(lastNode.chunkMod.id).asInstanceOf[Vector[(T, U)]]

      val newTailMod = datastore.createMod[ChunkListNode[T, U]](null)
      val chunkMod = datastore.createMod(Vector[(T, U)]((key -> value)))
      val newNode = new ChunkListNode(chunkMod, newTailMod, chunkSizer(value))

      count += datastore.updateMod(tailMod.id, newNode, respondTo)

      lastNodeMod = tailMod
      tailMod = newTailMod
    } else {
      val lastChunk =
        datastore.getMod(lastNode.chunkMod.id).asInstanceOf[Vector[(T, U)]]
      val chunkMod = datastore.createMod(lastChunk :+ (key -> value))
      val size = lastNode.size + chunkSizer(value)

      val newNode = new ChunkListNode(chunkMod, tailMod, size)
      count += datastore.updateMod(lastNodeMod.id, newNode, respondTo)
    }

    count
  }

  def update(key: T, value: U, respondTo: ActorRef): Int = {
    var node = datastore.getMod(list.head.id).asInstanceOf[ChunkListNode[T, U]]

    var count = 0
    var found = false
    while (!found && node != null) {
      val chunk = datastore.getMod(node.chunkMod.id).asInstanceOf[Vector[(T, U)]]

      val newChunk = chunk.map{ case (_key, _value) => {
        if (!found && key == _key) {
          found = true
          (_key -> value)
        } else {
          (_key -> _value)
        }
      }}

      if (found) {
        count += datastore.updateMod(node.chunkMod.id, newChunk, respondTo)
      } else {
        node = datastore.getMod(node.nextMod.id).asInstanceOf[ChunkListNode[T, U]]
      }
    }

    count
  }

  def remove(key: T, respondTo: ActorRef): Int = {
    var previousNode: ChunkListNode[T, U] = null
    var previousMod: Mod[ChunkListNode[T, U]] = null
    var mod = list.head
    var node = datastore.getMod(list.head.id).asInstanceOf[ChunkListNode[T, U]]

    var count = 0
    var found = false
    while (!found && node != null) {
      val chunk = datastore.getMod(node.chunkMod.id).asInstanceOf[Vector[(T, U)]]

      val newChunk = chunk.filter{ case (_key, _value) => {
        if (!found && key == _key) {
          found = true
          false
        } else {
          true
        }
      }}

      if (found) {
        if (newChunk.size == 0) {
          val lastNode = datastore.getMod(lastNodeMod.id)
          if (node == lastNode) {
            lastNodeMod = previousMod
          }

          val nextNode = datastore.getMod(node.nextMod.id)
            .asInstanceOf[ChunkListNode[T, U]]

          if (previousNode == null) {
            count += datastore.updateMod(list.head.id, nextNode, respondTo)
          } else {
            count += datastore.updateMod(previousNode.nextMod.id,
                                         nextNode,
                                         respondTo)
          }
        } else {
          count += datastore.updateMod(node.chunkMod.id, newChunk, respondTo)
        }
      } else {
        previousNode = node
        previousMod = mod
        node = datastore.getMod(node.nextMod.id)
          .asInstanceOf[ChunkListNode[T, U]]
        mod = node.nextMod
      }
    }

    count
  }

  def contains(key: T): Boolean = {
    var found = false
    var innerNode = datastore.getMod(list.head.id)
      .asInstanceOf[ChunkListNode[T, U]]

    while (innerNode != null && !found) {
      val chunk = datastore.getMod(innerNode.chunkMod.id)
	.asInstanceOf[Vector[(T, U)]]
      chunk.map{ case (_key, _value) =>
	if (key == _key)
	  found = true
      }

      innerNode = datastore.getMod(innerNode.nextMod.id)
	.asInstanceOf[ChunkListNode[T, U]]
    }

    found
  }

  def getModifiable(): Any = list
}
