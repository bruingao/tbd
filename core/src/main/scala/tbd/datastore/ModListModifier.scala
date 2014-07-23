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

import scala.collection.mutable.{ArrayBuffer, Map}
import scala.concurrent.{Future, Promise}

import tbd.ListConf
import tbd.mod._


class ModListModifier(
    _datastore: Datastore,
    conf: ListConf) extends Modifier(_datastore) {
  val modList =
    new ModList[Any, Any](datastore.createMod[ModListNode[Any, Any]](null))

  def insert(key: Any, value: Any): ArrayBuffer[Future[String]] = {
    var innerNode = datastore.getMod(modList.head.id)
      .asInstanceOf[ModListNode[Any, Any]]
    var previousNode: ModListNode[Any, Any] = null
    while (innerNode != null) {
      previousNode = innerNode
      innerNode = datastore.getMod(innerNode.next.id)
        .asInstanceOf[ModListNode[Any, Any]]
    }

    val newTail = datastore.createMod[ModListNode[Any, Any]](null)
    val newNode = new ModListNode((key, value), newTail)

    if (previousNode != null) {
      datastore.updateMod(previousNode.next.id, newNode)
    } else {
      datastore.updateMod(modList.head.id, newNode)
    }
  }

  def update(key: Any, value: Any): ArrayBuffer[Future[String]] = {
    var futures = ArrayBuffer[Future[String]]()
    var found = false
    var innerNode = datastore.getMod(modList.head.id)
      .asInstanceOf[ModListNode[Any, Any]]
    var previousNode: ModListNode[Any, Any] = null

    while (innerNode != null && !found) {
      if (innerNode.value._1 == key) {
        val newNode = new ModListNode((key, value), innerNode.next)

	if (previousNode != null) {
	  futures = datastore.updateMod(previousNode.next.id, newNode)
	} else {
	  futures = datastore.updateMod(modList.head.id, newNode)
	}
	found = true
      } else {
	previousNode = innerNode
        innerNode = datastore.getMod(innerNode.next.id)
          .asInstanceOf[ModListNode[Any, Any]]
      }
    }

    if (!found) {
      print("Didn't find value to remove.")
    }

    futures
  }

  def remove(key: Any): ArrayBuffer[Future[String]] = {
    var futures = ArrayBuffer[Future[String]]()
    var found = false
    var innerNode = datastore.getMod(modList.head.id)
      .asInstanceOf[ModListNode[Any, Any]]

    var previousNode: ModListNode[Any, Any] = null
    while (innerNode != null && !found) {
      if (innerNode.value._1 == key) {
        if (previousNode != null) {
          futures = datastore.updateMod(previousNode.next.id,
                                        datastore.getMod(innerNode.next.id))
        } else {
          futures = datastore.updateMod(modList.head.id,
                                        datastore.getMod(innerNode.next.id))
        }

        datastore.removeMod(innerNode.next.id)

	found = true
      } else {
        previousNode = innerNode
        innerNode = datastore.getMod(innerNode.next.id)
          .asInstanceOf[ModListNode[Any, Any]]
      }
    }

    if (!found) {
      print("Didn't find value to remove.")
    }

    futures
  }

  def contains(key: Any): Boolean = {
    var found = false
    var innerNode = datastore.getMod(modList.head.id)
      .asInstanceOf[ModListNode[Any, Any]]

    while (innerNode != null && !found) {
      if (innerNode.value._1 == key) {
	found = true
      } else {
        innerNode = datastore.getMod(innerNode.next.id)
          .asInstanceOf[ModListNode[Any, Any]]
      }
    }

    found
  }

  def getModifiable(): AdjustableList[Any, Any] = {
    modList
  }
}
