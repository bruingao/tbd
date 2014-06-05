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
import akka.event.LoggingAdapter
import scala.collection.mutable.{Map, MutableList, Set, TreeSet}

import tbd.Changeable
import tbd.Constants._
import tbd.master.Master
import tbd.memo.MemoEntry
import tbd.mod.{Dest, Mod}
import tbd.worker.Worker

class DDG(log: LoggingAdapter, id: String, worker: Worker) {
  var root = new RootNode(id)
  val reads = Map[ModId, Set[ReadNode]]()
  val pars = Map[ActorRef, ParNode]()

  var updated = TreeSet[Node]()((new TimestampOrdering()).reverse)

  val ordering = new Ordering()

  def addRead(
      mod: Mod[Any],
      parent: Node,
      reader: Any => Changeable[Any]): ReadNode = {
    val timestamp = nextTimestamp(parent)
    val readNode = new ReadNode(mod, parent, timestamp, reader)
    parent.addChild(readNode)

    if (reads.contains(mod.id)) {
      reads(mod.id) += readNode.asInstanceOf[ReadNode]
    } else {
      reads(mod.id) = Set(readNode.asInstanceOf[ReadNode])
    }

    readNode
  }

  def addReadMod(
      mod: Mod[Any],
      parent: Node,
      reader: (Dest[Any], Any) => Changeable[Any]): ReadModNode = {
    val timestamp = nextTimestamp(parent)
    val readNode = new ReadModNode(mod, parent, timestamp, reader)
    parent.addChild(readNode)

    if (reads.contains(mod.id)) {
      reads(mod.id) += readNode.asInstanceOf[ReadNode]
    } else {
      reads(mod.id) = Set(readNode.asInstanceOf[ReadNode])
    }

    readNode
  }

  def addWrite(mod: Mod[Any], parent: Node): Node = {
    val timestamp = nextTimestamp(parent)
    val writeNode = new WriteNode(mod, parent, timestamp)

    parent.addChild(writeNode)

    writeNode
  }

  def addPar(workerRef1: ActorRef, workerRef2: ActorRef, parent: Node) {
    val timestamp = nextTimestamp(parent)

    val parNode = new ParNode(workerRef1, workerRef2, parent, timestamp)
    parent.addChild(parNode)

    pars(workerRef1) = parNode
    pars(workerRef2) = parNode
  }

  def addMemo(parent: Node, signature: List[Any]): MemoNode = {
    val timestamp = nextTimestamp(parent)
    val memoNode = new MemoNode(parent, timestamp, signature)
    parent.addChild(memoNode)
    memoNode
  }

  def nextTimestamp(parent: Node): Timestamp = {
    if (parent.children.size == 0) {
      ordering.after(parent.timestamp)
    } else {
      ordering.after(parent.children.last.endTime)
    }
  }

  /**
   * Returns the timestamp for the next node in the graph after the execution
   * this node represents completes, which is the timestamp of the node's next
   * older sibling, unless it has no older siblings then its the node's parent's
   * next older sibling, recursively up the tree until an older sibling is found.
   */
  def getTimestampAfter(node: Node): Timestamp = {
    var previousChild: Node = null
    var ret: Timestamp = null

    if (node.parent == null) {
      Timestamp.MAX_TIMESTAMP
    } else {
      for (child <- node.parent.children) {
	if (previousChild == node) {
	  ret = child.timestamp
	}

	previousChild = child
      }

      if (ret == null) {
	getTimestampAfter(node.parent)
      } else {
	ret
      }
    }
  }

  def modUpdated(modId: ModId) {
    assert(reads.contains(modId))
    for (readNode <- reads(modId)) {
      if (!readNode.updated) {
        updated += readNode
      }

      readNode.updated = true
    }
  }

  // Pebbles a par node. Returns true iff the pebble did not already exist.
  def parUpdated(workerRef: ActorRef): Boolean = {
    val parNode = pars(workerRef)
    if (!parNode.pebble1 && !parNode.pebble2) {
      updated += parNode
      parNode.updated = true
    }

    if (parNode.workerRef1 == workerRef) {
      val ret = !parNode.pebble1
      parNode.pebble1 = true
      ret
    } else {
      val ret = !parNode.pebble2
      parNode.pebble2 = true
      ret
    }
  }

  /**
   * Called before a read is reexecuted, the descendents of this node are
   * cleaned up, up to the first memo nodes, which are returned so that
   * they can be reattached or cleaned up later.
   */
  def cleanupRead(subtree: Node): MutableList[Node] = {
    val ret = new MutableList[Node]()

    for (child <- subtree.children) {
      child.parent = null
    }

    ret ++= subtree.children

    subtree.children.clear()

    ret
  }

  def cleanupSubtree(subtree: Node) {
    for (child <- subtree.children) {
      cleanupSubtree(child)
    }

    cleanup(subtree)
  }

  private def cleanup(node: Node) {
    if (node.isInstanceOf[ReadNode]) {
      val readNode = node.asInstanceOf[ReadNode]
      reads(readNode.mod.id) -= readNode
    } else if (node.isInstanceOf[MemoNode]) {
      val signature = node.asInstanceOf[MemoNode].signature

      var toRemove: MemoEntry = null
      for (memoEntry <- worker.memoTable(signature)) {
        if (toRemove == null && memoEntry.node.timestamp == node.timestamp) {
          toRemove = memoEntry
        }
      }

      worker.memoTable(signature) -= toRemove
      if (worker.memoTable(signature).size == 0) {
	worker.memoTable -= signature
      }
    }

    node.updated = false
    ordering.remove(node.timestamp)

    node.children.clear()
  }

  def attachSubtree(parent: Node, subtree: Node) {
    if (subtree.parent != null) {
      subtree.parent.removeChild(subtree)

      var oldParent = subtree.parent
      while (oldParent != null) {
        oldParent.matchableInEpoch = Master.epoch + 1
        oldParent = oldParent.parent
      }
    }

    parent.addChild(subtree)
    subtree.parent = parent
  }

  override def toString = {
    root.toString("")
  }

  def toString(prefix: String): String = {
    root.toString(prefix)
  }
}
