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
package tbd.worker

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.pattern.ask
import scala.collection.mutable.{ArrayBuffer, Map, MutableList, Set}
import scala.concurrent.{Await, Future, Promise}
import scala.util.Try

import tbd.Constants._
import tbd.TBD
import tbd.ddg._
import tbd.master.Master
import tbd.memo.MemoEntry
import tbd.messages._
import tbd.mod.{AdjustableList}

object Worker {
  def props(id: String, datastoreRef: ActorRef, parent: ActorRef): Props =
    Props(classOf[Worker], id, datastoreRef, parent)
}

class Worker(_id: String, _datastoreRef: ActorRef, parent: ActorRef)
  extends Actor with ActorLogging {
  import context.dispatcher

  //log.info("Worker " + id + " launched")
  val id = _id

  val datastoreRef = _datastoreRef
  val ddg = new DDG(log, id, this)
  val memoTable = Map[List[Any], ArrayBuffer[MemoEntry]]()
  val readModTable = Map[List[Any], ReadModNode]()

  val adjustableLists = Set[AdjustableList[Any, Any]]()

  private val tbd = new TBD(id, this)

  var nextModId = 0

  def propagate(start: Timestamp = Timestamp.MIN_TIMESTAMP,
                end: Timestamp = Timestamp.MAX_TIMESTAMP): Future[Boolean] = {
    Future {
      var option = ddg.updated.find((node: Node) =>
        node.timestamp > start && node.timestamp < end)
      while (!option.isEmpty) {
        val node = option.get
        ddg.updated -= node

        if (node.updated) {
	  log.debug("propagating " + node)
          if (node.isInstanceOf[ReadNode]) {
            val readNode = node.asInstanceOf[ReadNode]

            val toCleanup = ddg.cleanupRead(readNode)

            val newValue = readNode.mod.read()

	    val oldCurrentParent = tbd.currentParent
            tbd.currentParent = readNode
	    val oldStart = tbd.reexecutionStart
	    tbd.reexecutionStart = readNode.timestamp
	    val oldEnd = tbd.reexecutionEnd
	    tbd.reexecutionEnd = readNode.endTime

            readNode.updated = false

	    if (readNode.isInstanceOf[ReadModNode]) {
	      val readModNode = readNode.asInstanceOf[ReadModNode]
	      var found = false

	      if (readNode.mod.replacedBy != null) {
		var replacedBy = readNode.mod.replacedBy
		while (replacedBy.replacedBy != null) {
		  replacedBy = replacedBy.replacedBy
		}

		if (readModTable.contains(List(readModNode.memoId, replacedBy.id)) &&
		    !tbd.updated(List(replacedBy.id))) {
		  val node = readModTable(List(readModNode.memoId, replacedBy.id))
		  val timestamp = node.timestamp
		  if (timestamp > tbd.reexecutionStart &&
		      timestamp < tbd.reexecutionEnd &&
		      node.matchableInEpoch <= Master.epoch) {
		    found = true
		    for (child <- node.children) {
		      ddg.attachSubtree(readNode, child)
		    }

		    readModNode.dest.mod.update(node.dest.mod.read())

		    node.matchableInEpoch = Master.epoch + 1

		    // This ensures that we won't match anything under the currently
		    // reexecuting read that comes before this memo node, since then
		    // the timestamps would be out of order.
		    tbd.reexecutionStart = node.endTime

		    val future = propagate(timestamp, node.endTime)
		    Await.result(future, DURATION)
		  }
		}
	      }

	      if (!found) {
		readModNode.modReader(readModNode.dest, newValue)
	      }
	    } else {
              readNode.reader(newValue)
	    }

	    tbd.currentParent = oldCurrentParent
	    tbd.reexecutionStart = oldStart
	    tbd.reexecutionEnd = oldEnd

            for (node <- toCleanup) {
              if (node.parent == null) {
                ddg.cleanupSubtree(node)
              }
            }
          } else {
            val parNode = node.asInstanceOf[ParNode]

            val future1 = parNode.workerRef1 ? PropagateMessage
            val future2 = parNode.workerRef2 ? PropagateMessage

            parNode.pebble1 = false
            parNode.pebble2 = false

            Await.result(future1, DURATION)
            Await.result(future2, DURATION)
          }
        }

        option = ddg.updated.find((node: Node) =>
          node.timestamp > start && node.timestamp < end)
      }

      true
    }

  }

  def receive = {
    case ModUpdatedMessage(modId: ModId, finished: Future[String]) => {
      ddg.modUpdated(modId)
      tbd.updatedMods += modId

      parent ! PebbleMessage(self, modId, finished)
    }

    case RunTaskMessage(task: Task) => {
      sender ! task.func(tbd)
    }

    case PebbleMessage(workerRef: ActorRef, modId: ModId, finished: Promise[String]) => {
      val newPebble = ddg.parUpdated(workerRef)

      if (newPebble) {
        parent ! PebbleMessage(self, modId, finished)
      } else {
        finished.success("done")
      }
    }

    case PropagateMessage => {
      tbd.initialRun = false
      val respondTo = sender
      val future = propagate()
      future.onComplete((t: Try[Boolean]) => {
	tbd.updatedMods.clear()
        respondTo ! "done"
      })
    }

    case GetDDGMessage => {
      sender ! ddg
    }

    case DDGToStringMessage(prefix: String) => {
      sender ! ddg.toString(prefix)
    }

    case CleanupWorkerMessage => {
      val datastoreFuture = datastoreRef ? CleanUpMessage(self, adjustableLists)

      val futures = Set[Future[Any]]()
      for ((actorRef, parNode) <- ddg.pars) {
        futures += actorRef ? CleanupWorkerMessage
        actorRef ! akka.actor.PoisonPill
      }

      for (future <- futures) {
        Await.result(future, DURATION)
      }

      Await.result(datastoreFuture, DURATION)

      sender ! "done"
    }

    case x => {
      log.warning(id + " received unhandled message " + x + " from " + sender)
    }
  }
}
