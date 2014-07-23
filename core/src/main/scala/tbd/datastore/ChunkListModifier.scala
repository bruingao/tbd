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
import scala.concurrent.Future

import tbd.ListConf
import tbd.mod.{ChunkList, ChunkListNode, Mod}

class ChunkListModifier(
    _datastore: Datastore,
    conf: ListConf) extends Modifier(_datastore) {
  protected var tailMod: Mod[ChunkListNode[Any, Any]] = null

  // Contains the last ChunkListNode before the tail node.
  protected var lastNodeMod: Mod[ChunkListNode[Any, Any]] = null

  val list = initialize()

  private def initialize(): ChunkList[Any, Any] = {
    var tail = datastore.createMod[ChunkListNode[Any, Any]](null)
    tailMod = tail

    tail = datastore.createMod(new ChunkListNode(Vector[(Any, Any)](), tail, 0))
    lastNodeMod = tail

    /*if (conf.file != "") {
      var chunk = Vector[(Any, Any)]()
      var size = 0

      val elems = scala.xml.XML.loadFile(conf.file)
      (elems \\ "elem").map(elem => {
	(elem \\ "key").map(key => {
	  (elem \\ "value").map(value => {
	    if (size >= conf.chunkSize) {
	      tail = datastore.createMod(new ChunkListNode(chunk, tail, size))
	      chunk = Vector[(Any, Any)]()
	      size = 0
	    }

	    chunk = chunk :+ (key.text -> value.text)
	    size += conf.chunkSizer(value.text)
	  })
	})
      })

      if (size > 0) {
	tail = datastore.createMod(new ChunkListNode(chunk, tail, size))
      }
    }*/

    new ChunkList[Any, Any](tail)
  }

  def insert(key: Any, value: Any): ArrayBuffer[Future[String]] = {
    val lastNode = datastore.getMod(lastNodeMod.id)
      .asInstanceOf[ChunkListNode[Any, Any]]

    var futures = ArrayBuffer[Future[String]]()
    if (lastNode == null) {
      val chunk = Vector[(Any, Any)]((key -> value))
      val size = conf.chunkSizer(value)

      val newNode = new ChunkListNode(chunk, tailMod, size)
      futures = datastore.updateMod(lastNodeMod.id, newNode)
    } else if (lastNode.size >= conf.chunkSize) {
      val newTailMod = datastore.createMod[ChunkListNode[Any, Any]](null)
      val chunk = Vector[(Any, Any)]((key -> value))
      val newNode = new ChunkListNode(chunk, newTailMod, conf.chunkSizer(value))

      futures = datastore.updateMod(tailMod.id, newNode)

      lastNodeMod = tailMod
      tailMod = newTailMod
    } else {
      val chunk = lastNode.chunk :+ (key -> value)
      val size = lastNode.size + conf.chunkSizer(value)

      val newNode = new ChunkListNode(chunk, tailMod, size)
      futures = datastore.updateMod(lastNodeMod.id, newNode)
    }

    futures
  }

  def update(key: Any, value: Any): ArrayBuffer[Future[String]] = {
    var node = datastore.getMod(list.head.id).asInstanceOf[ChunkListNode[Any, Any]]
    var previousNode: ChunkListNode[Any, Any] = null

    var futures = ArrayBuffer[Future[String]]()
    var found = false
    var oldValue: Any = null.asInstanceOf[Any]
    while (!found && node != null) {

      val newChunk = node.chunk.map{ case (_key, _value) => {
        if (!found && key == _key) {
          found = true
          oldValue = _value
          (_key -> value)
        } else {
          (_key -> _value)
        }
      }}

      if (found) {
        val newSize = node.size + conf.chunkSizer(value) - conf.chunkSizer(oldValue)
        val newNode = new ChunkListNode(newChunk, node.nextMod, newSize)

        if (previousNode != null) {
          futures = datastore.updateMod(previousNode.nextMod.id, newNode)
        } else {
          futures = datastore.updateMod(list.head.id, newNode)
        }
      } else {
        previousNode = node
        node = datastore.getMod(node.nextMod.id).asInstanceOf[ChunkListNode[Any, Any]]
      }
    }

    futures
  }

  def remove(key: Any): ArrayBuffer[Future[String]] = {
    var previousNode: ChunkListNode[Any, Any] = null
    var previousMod: Mod[ChunkListNode[Any, Any]] = null
    var mod = list.head
    var node = datastore.getMod(list.head.id).asInstanceOf[ChunkListNode[Any, Any]]

    var futures = ArrayBuffer[Future[String]]()
    var found = false
    var oldValue: Any = null.asInstanceOf[Any]
    while (!found && node != null) {
      val newChunk = node.chunk.filter{ case (_key, _value) => {
        if (!found && key == _key) {
          oldValue = _value
          found = true
          false
        } else {
          true
        }
      }}

      if (found) {
        if (newChunk.size == 0) {
          val lastNode = datastore.getMod(lastNodeMod.id)
            .asInstanceOf[ChunkListNode[Any, Any]]
          val nextNode = datastore.getMod(node.nextMod.id)
            .asInstanceOf[ChunkListNode[Any, Any]]

          if (node == lastNode) {
            assert(nextNode == null)
            lastNodeMod = previousMod
          } else if (lastNodeMod.id == node.nextMod.id) {
            lastNodeMod = previousNode.nextMod
          }

          if (previousNode == null) {
            futures = datastore.updateMod(list.head.id, nextNode)
          } else {
            futures = datastore.updateMod(previousNode.nextMod.id,
                                          nextNode)
          }
        } else {
          val newSize = node.size - conf.chunkSizer(oldValue)
          val newNode = new ChunkListNode(newChunk, node.nextMod, newSize)

          if (previousNode == null) {
            futures = datastore.updateMod(list.head.id, newNode)
          } else {
            futures = datastore.updateMod(previousNode.nextMod.id, newNode)
          }
        }
      } else {
        previousNode = node
        previousMod = mod
        node = datastore.getMod(node.nextMod.id)
          .asInstanceOf[ChunkListNode[Any, Any]]
        mod = node.nextMod
      }
    }

    futures
  }

  def contains(key: Any): Boolean = {
    var found = false
    var innerNode = datastore.getMod(list.head.id)
      .asInstanceOf[ChunkListNode[Any, Any]]

    while (innerNode != null && !found) {
      innerNode.chunk.map{ case (_key, _value) =>
	if (key == _key)
	  found = true
      }

      innerNode = datastore.getMod(innerNode.nextMod.id)
	.asInstanceOf[ChunkListNode[Any, Any]]
    }

    found
  }

  def getModifiable(): Any = list
}
