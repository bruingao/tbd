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
import scala.collection.mutable.{ArrayBuffer, Map, MutableList, PriorityQueue, Set}

import tbd.Changeable
import tbd.memo.MemoEntry
import tbd.mod.{Mod, ModId}
import tbd.worker.Worker

class DDG(log: LoggingAdapter, id: String, worker: Worker) {
  var root = new RootNode(id)
  val reads = Map[ModId, Set[ReadNode]]()
  val pars = Map[ActorRef, ParNode]()

  var lastRemovedMemo: MemoNode = null

  def addRead(
      mod: Mod[Any],
      parent: Node,
      reader: Any => Changeable[Any]): Node = {
    val readNode = new ReadNode(mod, parent, reader)
    parent.addChild(readNode)

    if (reads.contains(mod.id)) {
      reads(mod.id) += readNode
    } else {
      reads(mod.id) = Set(readNode)
    }

    readNode
  }

  def addWrite(mod: Mod[Any], parent: Node): Node = {
    val writeNode = new WriteNode(mod, parent)

    parent.addChild(writeNode)

    writeNode
  }

  def addPar(workerRef1: ActorRef, workerRef2: ActorRef, parent: Node) {
    val parNode = new ParNode(workerRef1, workerRef2, parent)
    parent.addChild(parNode)

    pars(workerRef1) = parNode
    pars(workerRef2) = parNode
  }

  def addMemo(parent: Node, signature: List[Any]): Node = {
    val memoNode = new MemoNode(parent, signature)
    parent.addChild(memoNode)
    memoNode
  }

  def modUpdated(modId: ModId) {
    assert(reads.contains(modId))
    for (readNode <- reads(modId)) {
      readNode.updated = true
      pebble(readNode)
    }
  }

  private def pebble(node: Node) {
    if (!node.pebble && node.parent != null) {
      pebble(node.parent)
    }

    node.pebble = true
  }

  // Pebbles a par node. Returns true iff the pebble did not already exist.
  def parUpdated(workerRef: ActorRef): Boolean = {
    val parNode = pars(workerRef)
    parNode.updated = true
    pebble(parNode)

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
   * Called before a read is reexecuted, the children of this node are attached
   * to a dummy node which is returned, so that they can either be memo matched
   * or cleaned up later.
   */
  def cleanupRead(subtree: Node): Node = {
    val dummy = new RootNode(null)
    for (child <- subtree.children) {
      child.parent = dummy
    }
    subtree.children.clear()
    dummy
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
        if (toRemove == null && memoEntry.node == node) {
          toRemove = memoEntry
        }
      }
      worker.memoTable(signature) -= toRemove
    }

    node.children.clear()
  }

  def attachSubtree(parent: Node, subtree: Node) {
    assert(subtree.parent != null)
    var oldParent = subtree.parent
    while (oldParent != null) {
      val oldParentParent = oldParent.parent
      oldParent.matchable = false
      oldParent = oldParentParent
    }

    //subtree.parent.removeChild(subtree)

    parent.addChild(subtree)
    subtree.parent = parent

    if (subtree.pebble) {
      pebble(subtree.parent)
    }
  }

  def descendedFrom(node: Node, parent: Node): Boolean = {
    if (node == null) {
      false
    } else if (node.parent == parent) {
      true
    } else if (node.parent == root) {
      false
    } else {
      descendedFrom(node.parent, parent)
    }
  }

  var currentNode = root
  def getUpdated(): Node = {
    def innerGetUpdated(node: Node): Node = {
      if (node.updated) {
        node.pebble = false
        node
      } else {
        var updated: Node = null
        var pebble = false
        for (child <- node.children) {
          if (updated == null && child.pebble) {
            updated = innerGetUpdated(child)
          }

          if (child.pebble) {
            pebble = true
          }
        }

        node.pebble = pebble

        updated
      }
    }

    innerGetUpdated(root)
  }

  override def toString = {
    root.toString("")
  }

  def toString(prefix: String): String = {
    root.toString(prefix)
  }
}
