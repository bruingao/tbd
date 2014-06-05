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

import tbd.mod._

class ModListModifier[T, U](
    _datastore: Datastore,
    table: Map[Any, Any] = Map[Any, Any]()) extends Modifier[T, U](_datastore) {
  val modList = initialize()

  private def initialize(): ModList[T, U] = {
    var tail = datastore.createMod[ModListNode[T, U]](null)

    for (elem <- table) {
    //for (elem <- scala.collection.immutable.TreeMap(table.asInstanceOf[Map[Int, U]].toSeq: _*)) {
      //println("creating node " + elem._1)
      val newNode = new ModListNode[T, U](elem.asInstanceOf[(T, U)], tail)
      tail = datastore.createMod(newNode)
    }

    new ModList[T, U](tail)
  }

  def insert(key: T, value: U): ArrayBuffer[Future[String]] = {
    var innerNode = datastore.getMod(modList.head.id)
      .asInstanceOf[ModListNode[T, U]]
    var previousNode: ModListNode[T, U] = null
    while (innerNode != null) {
      previousNode = innerNode
      innerNode = datastore.getMod(innerNode.next.id)
        .asInstanceOf[ModListNode[T, U]]
    }

    val newTail = datastore.createMod[ModListNode[T, U]](null)
    val newNode = new ModListNode((key, value), newTail)

    if (previousNode != null) {
      datastore.updateMod(previousNode.next.id, newNode)
    } else {
      datastore.updateMod(modList.head.id, newNode)
    }
  }

  def update(key: T, value: U): ArrayBuffer[Future[String]] = {
    var futures = ArrayBuffer[Future[String]]()
    var found = false
    var innerNode = datastore.getMod(modList.head.id)
      .asInstanceOf[ModListNode[T, U]]
    var previousNode: ModListNode[T, U] = null

    while (innerNode != null && !found) {
      if (innerNode.value._1 == key) {
	innerNode.value  = (key, value)

	if (previousNode != null) {
	  futures = datastore.updateMod(previousNode.next.id, previousNode.next.value)
	} else {
	  futures = datastore.updateMod(modList.head.id, modList.head.value)
	}
	found = true
      } else {
	previousNode = innerNode
        innerNode = datastore.getMod(innerNode.next.id)
          .asInstanceOf[ModListNode[T, U]]
      }
    }

    if (!found) {
      print("Didn't find value to remove.")
    }

    futures
  }

  def remove(key: T): ArrayBuffer[Future[String]] = {
    var futures = ArrayBuffer[Future[String]]()
    var found = false
    var innerNode = datastore.getMod(modList.head.id)
      .asInstanceOf[ModListNode[T, U]]

    var previousNode: ModListNode[T, U] = null
    while (innerNode != null && !found) {
      if (innerNode.value._1 == key) {
        if (previousNode != null) {
	  var replacedBy: Mod[_] = innerNode.next
	  while (replacedBy.replacedBy != null) {
	    replacedBy = replacedBy.replacedBy
	  }
          futures = datastore.updateMod(previousNode.next.id,
                                        datastore.getMod(innerNode.next.id),
					replacedBy)
        } else {
	  var replacedBy: Mod[_] = innerNode.next
	  while (replacedBy.replacedBy != null) {
	    replacedBy = replacedBy.replacedBy
	  }

          futures = datastore.updateMod(modList.head.id,
                                        datastore.getMod(innerNode.next.id),
					replacedBy)
        }

        datastore.removeMod(innerNode.next.id)

	found = true
      } else {
        previousNode = innerNode
        innerNode = datastore.getMod(innerNode.next.id)
          .asInstanceOf[ModListNode[T, U]]
      }
    }

    if (!found) {
      print("Didn't find value to remove.")
    }

    futures
  }

  def contains(key: T): Boolean = {
    var found = false
    var innerNode = datastore.getMod(modList.head.id)
      .asInstanceOf[ModListNode[T, U]]

    while (innerNode != null && !found) {
      if (innerNode.value._1 == key) {
	found = true
      } else {
        innerNode = datastore.getMod(innerNode.next.id)
          .asInstanceOf[ModListNode[T, U]]
      }
    }

    found
  }

  def getModifiable(): AdjustableList[T, U] = {
    modList
  }
}
