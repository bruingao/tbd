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
import tbd.mod.Mod

abstract class Node(aParent: Node) {
  var parent = aParent

  var children = MutableList[Node]()

  var pebble: Boolean = false
  var updated = false

  def addChild(child: Node) {
    children += child
  }

  def removeChild(child: Node) {
    children = children.filter(_ != child)
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

class ReadNode(aMod: Mod[Any], aParent: Node, aReader: Any => Changeable[Any])
    extends Node(aParent) {
  val mod: Mod[Any] = aMod
  val reader = aReader

  override def toString(prefix: String) = {
    val value = 
      if (Main.debug) {
	" value=" + mod
      } else {
	""
      }

    prefix + "ReadNode modId=(" + mod.id + ") " +
      value + " updated=(" + updated + ") pebble=" + pebble + " " + this +
      super.toString(prefix)
  }
}

class WriteNode(aMod: Mod[Any], aParent: Node)
    extends Node(aParent) {
  val mod: Mod[Any] = aMod

  override def toString(prefix: String) = {
    val value = 
      if (Main.debug) {
	" value=" + mod
      } else {
	""
      }

    prefix + "WriteNode modId=(" + mod.id + ") " +
      value + super.toString(prefix)
  }
}

class ParNode(
    aWorkerRef1: ActorRef,
    aWorkerRef2: ActorRef,
    aParent: Node) extends Node(aParent) {
  val workerRef1 = aWorkerRef1
  val workerRef2 = aWorkerRef2

  var pebble1 = false
  var pebble2 = false

  override def toString(prefix: String) = {
    val future1 = workerRef1 ? DDGToStringMessage(prefix + "|")
    val future2 = workerRef2 ? DDGToStringMessage(prefix + "|")

    val output1 = Await.result(future1, DURATION).asInstanceOf[String]
    val output2 = Await.result(future2, DURATION).asInstanceOf[String]

    prefix + "ParNode pebbles=(" + pebble1 + ", " +
      pebble2 + ")\n" + output1 + "\n" + output2 + super.toString(prefix)
  }
}

class MemoNode(
    aParent: Node,
    aSignature: List[Any]) extends Node(aParent) {
  val signature = aSignature

  override def toString(prefix: String) = {
    prefix + "MemoNode signature=" + signature +
      " pebble=" + pebble + super.toString(prefix)
  }
}

class RootNode(id: String) extends Node(null) {
  override def toString(prefix: String) = {
    prefix + "RootNode id=(" + id + ") pebble=" + pebble + " " + this + super.toString(prefix)
  }
}
