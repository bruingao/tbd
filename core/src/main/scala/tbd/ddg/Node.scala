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
import akka.pattern.ask
import scala.collection.mutable.MutableList
import scala.concurrent.Await

import tbd.Changeable
import tbd.Constants._
import tbd.master.Main
import tbd.messages._
import tbd.mod.{Dest, Mod}

abstract class Node(var parent: Node, val timestamp: Timestamp) {
  var endTime: Timestamp = null
  var stacktrace =
    if (Main.debug)
      Thread.currentThread().getStackTrace()
    else
      null

  var children = MutableList[Node]()

  var updated = false

  // The earliest epoch in which this node may be matched, if it is a MemoNode.
  // This is increased above the current epoch whenever the node is matched, so
  // that it won't be matched again in this round of change propagation.
  var matchableInEpoch = 0

  var currentDest: Dest[Any] = null

  var currentDest2: Dest[Any] = null

  def addChild(child: Node) {
    children += child
  }

  def removeChild(child: Node) {
    children = children.filter(_ != child)
  }

  override def equals(obj: Any): Boolean = {
    obj.isInstanceOf[Node] && obj.asInstanceOf[Node].timestamp == timestamp
  }

  def toString(prefix: String): String = {
    if (children.isEmpty) {
	    ""
    } else if (children.size == 1) {
	    "\n" + children.head.toString(prefix + "-")
    } else {
      var ret = ""
      for (child <- children) {
        ret += "\n" + child.toString(prefix + "-")
      }
      ret
    }
  }
}

class ReadNode(
    val mod: Mod[Any],
    _parent: Node,
    _timestamp: Timestamp,
    val reader: Any => Changeable[Any])
      extends Node(_parent, _timestamp) {

  override def toString(prefix: String) = {
    prefix + this + " modId=(" + mod.id + ") " + " time=" + timestamp + " to " + endTime +
      " value=" + mod + " updated=(" + updated + ")" + super.toString(prefix)
  }
}

class WriteNode(val mod: Mod[Any], _parent: Node, _timestamp: Timestamp)
    extends Node(_parent, _timestamp) {
  var mod2: Mod[Any] = null

  override def toString(prefix: String) = {
    prefix + "WriteNode modId=(" + mod.id + ") " +
      " value=" + mod + " time=" + timestamp + super.toString(prefix)
  }
}

class ParNode(
    val workerRef1: ActorRef,
    val workerRef2: ActorRef,
    _parent: Node,
    _timestamp: Timestamp) extends Node(_parent, _timestamp) {

  var pebble1 = false
  var pebble2 = false

  override def toString(prefix: String) = {
    val future1 = workerRef1 ? DDGToStringMessage(prefix + "|")
    val future2 = workerRef2 ? DDGToStringMessage(prefix + "|")

    val output1 = Await.result(future1, DURATION).asInstanceOf[String]
    val output2 = Await.result(future2, DURATION).asInstanceOf[String]

    prefix + "ParNode time=" + timestamp + " pebbles=(" + pebble1 + ", " +
      pebble2 + ")\n" + output1 + "\n" + output2 + super.toString(prefix)
  }
}

class MemoNode(
    _parent: Node,
    _timestamp: Timestamp,
    val signature: Seq[Any]) extends Node(_parent, _timestamp) {
  var value: Any = null

  override def toString(prefix: String) = {
    prefix + "MemoNode time=" + timestamp + " to " + endTime + " signature=" + signature +
      super.toString(prefix)
  }
}

class RootNode(id: String) extends Node(null, null) {
  override def toString(prefix: String) = {
    prefix + "RootNode id=(" + id + ")" + super.toString(prefix)
  }
}
