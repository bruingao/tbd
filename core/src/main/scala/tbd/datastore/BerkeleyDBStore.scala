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

import akka.actor.{ActorRef, ActorContext, Props}
import akka.pattern.ask
import scala.collection.mutable.Map
import scala.concurrent.Await

import tbd.Constants._
import tbd.messages._
import tbd.mod.Mod

class LRUNode(
  val key: ModId,
  var value: Any,
  var previous: LRUNode,
  var next: LRUNode
)

class BerkeleyDBStore(cacheSize: Int, context: ActorContext) extends KVStore {
  val numPartitions = 1

  val partitions = Map[Int, ActorRef]()
  for (i <- 0 until numPartitions) {
    partitions(i) = context.actorOf(Props[BerkeleyDBActor])
  }

  private val values = Map[ModId, LRUNode]()
  private val tail = new LRUNode(null, null, null, null)
  private var head = tail

  def put(key: ModId, value: Any) {
    if (values.contains(key)) {
      values(key).value = value
    } else {
      val newNode = new LRUNode(key, value, null, head)
      values(key) = newNode

      head.previous = newNode
      head = newNode

      if (values.size > cacheSize) {
	evict()
      }
    }
  }

  private def evict() {
    while (values.size > cacheSize) {
      val toEvict = tail.previous

      getPartition(toEvict.key) ! DBPutMessage(toEvict.key, toEvict.value)

      values -= toEvict.key

      tail.previous = toEvict.previous
      toEvict.previous.next = tail
    }
  }

  def get(key: ModId): Any = {
    if (values.contains(key)) {
      values(key).value
    } else {
      val future = getPartition(key) ? DBGetMessage(key)
      Await.result(future, DURATION)
    }
  }

  def remove(key: ModId) {
    if (values.contains(key)) {
      values -= key
    }

    getPartition(key) ! DBDeleteMessage(key)
  }

  def contains(key: ModId): Boolean = {
    values.contains(key) || {
      val future = getPartition(key) ? DBContainsMessage(key)
      Await.result(future.mapTo[Boolean], DURATION)
    }
  }

  def shutdown() {
    for ((num, partition) <- partitions) {
      partition ! DBShutdownMessage()
    }
  }

  private def getPartition(key: ModId): ActorRef = {
    partitions(key.hashCode % numPartitions)
  }
}
