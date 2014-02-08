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
package tbd.ddg

import akka.actor.ActorRef
import scala.collection.mutable.Set

import tbd.Changeable
import tbd.mod.ModId

abstract class Node(aModId: ModId, aParent: Node, aTimestamp: Timestamp) {
  val modId: ModId = aModId
  val parent = aParent
  var timestamp: Timestamp = aTimestamp

  val children = Set[Node]()

  def addChild(child: Node) {
    children += child
  }

  def removeChild(child: Node) {
    children -= child
  }

  def name(): String

  def toString(prefix: String): String = {
    val childrenString =
      if (children.isEmpty) {
	      ""
      } else if (children.size == 1) {
	      "\n" + children.head.toString(prefix + "-")
      } else {
	      "\n" + children.map(_.toString(prefix + "-")).reduceLeft(_ + "\n" + _)
      }

    prefix + name() + "(" + modId + ") time = " + timestamp + " " + childrenString
  }
}

class ReadNode[T](aModId: ModId, aParent: Node, aTimestamp: Timestamp, aReader: T => Changeable[T])
    extends Node(aModId, aParent, aTimestamp) {
  var updated = false
  val reader = aReader

  def name() = "ReadNode (" + updated + ")"
}

class WriteNode(aModId: ModId, aParent: Node, aTimestamp: Timestamp)
    extends Node(aModId, aParent, aTimestamp) {
  def name() = "WriteNode"
}

class ParNode(
    aWorkerRef1: ActorRef,
    aWorkerRef2: ActorRef,
    aParent: Node,
    aTimestamp: Timestamp) extends Node(null, aParent, aTimestamp) {
  val workerRef1 = aWorkerRef1
  val workerRef2 = aWorkerRef2

  var pebble1 = false
  var pebble2 = false

  def name() = "ParNode (" + pebble1 + ", " + pebble2 + ")"
}

class RootNode extends Node(null, null, null) {
  def name() = "RootNode"
}
