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

import tbd.mod._

class DMLModifier[T, U](
    aDatastore: Datastore,
    table: Map[Any, Any] = Map[Any, Any]()) extends Modifier[T, U](aDatastore) {
  val doubleModList = initialize()

  private def initialize(): DoubleModList[T, U] = {
    var tail = datastore.createMod[DoubleModListNode[T, U]](null)

    for (elem <- table) {
      val newNode = new DoubleModListNode[T, U](
                  elem._1.asInstanceOf[T],
                  datastore.createMod(elem._2.asInstanceOf[U]), tail)
      tail = datastore.createMod(newNode)
    }

    new DoubleModList[T, U](tail)
  }

  def insert(key: T, value: U, respondTo: ActorRef): Int = {
    var innerNode = datastore.getMod(doubleModList.head.id)
      .asInstanceOf[DoubleModListNode[T, U]]
    var previousNode: DoubleModListNode[T, U] = null
    while (innerNode != null) {
      previousNode = innerNode
      innerNode = datastore.getMod(innerNode.next.id)
        .asInstanceOf[DoubleModListNode[T, U]]
    }

    val newTail = datastore.createMod[DoubleModListNode[T, U]](null)
    val newNode = new DoubleModListNode(key, datastore.createMod(value), newTail)

    if (previousNode != null) {
      datastore.updateMod(previousNode.next.id, newNode, respondTo)
    } else {
      datastore.updateMod(doubleModList.head.id, newNode, respondTo)
    }
  }

  def update(key: T, value: U, respondTo: ActorRef): Int = {
    var count = 0
    var found = false
    var innerNode = datastore.getMod(doubleModList.head.id)
      .asInstanceOf[DoubleModListNode[T, U]]

    while (innerNode != null && !found) {
      if (innerNode.key == key) {
	count = datastore.updateMod(innerNode.value.id, value, respondTo)
	found = true
      } else {
        innerNode = datastore.getMod(innerNode.next.id)
          .asInstanceOf[DoubleModListNode[T, U]]
      }
    }

    if (!found) {
      print("Didn't find value to remove.")
    }

    count
  }

  def remove(key: T, respondTo: ActorRef): Int = {
    var count = 0
    var found = false
    var innerNode = datastore.getMod(doubleModList.head.id)
      .asInstanceOf[DoubleModListNode[T, U]]

    var previousNode: DoubleModListNode[T, U] = null
    while (innerNode != null && !found) {
      if (innerNode.key == key) {
        if (previousNode != null) {
          count += datastore.updateMod(previousNode.next.id,
                                       datastore.tables("mods")(innerNode.next.id),
                                       respondTo)
        } else {
          count += datastore.updateMod(doubleModList.head.id,
                                       datastore.tables("mods")(innerNode.next.id),
                                       respondTo)
        }

        // Note: for now, we're not addressing what happens if you try to get
        // a value from the table that doesn't exist, so we don't need to
        // notify workers when a mod is removed.
        datastore.tables("mods") -= innerNode.next.id
	datastore.tables("mods") -= innerNode.value.id

	found = true
      } else {
        previousNode = innerNode
        innerNode = datastore.getMod(innerNode.next.id)
          .asInstanceOf[DoubleModListNode[T, U]]
      }
    }

    if (!found) {
      print("Didn't find value to remove.")
    }

    count
  }

  def contains(key: T): Boolean = {
    var found = false
    var innerNode = datastore.getMod(doubleModList.head.id)
      .asInstanceOf[DoubleModListNode[T, U]]

    while (innerNode != null && !found) {
      if (innerNode.key == key) {
	found = true
      } else {
        innerNode = datastore.getMod(innerNode.next.id)
          .asInstanceOf[DoubleModListNode[T, U]]
      }
    }

    found
  }

  def getModifiable(): AdjustableList[T, U] = {
    doubleModList
  }
}
