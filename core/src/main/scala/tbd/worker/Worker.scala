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
import scala.concurrent.{Await, Future}

import tbd.Constants._
import tbd.TBD
import tbd.ddg.{DDG, Node, ParNode, ReadNode}
import tbd.memo.MemoEntry
import tbd.messages._
import tbd.mod.{AdjustableList, ModId}

object Worker {
  def props(id: String, datastoreRef: ActorRef, parent: ActorRef): Props =
    Props(classOf[Worker], id, datastoreRef, parent)
}

class Worker(aId: String, aDatastoreRef: ActorRef, parent: ActorRef)
  extends Actor with ActorLogging {
  //log.info("Worker " + id + " launched")
  val id = aId

  val datastoreRef = aDatastoreRef
  val ddg = new DDG(log, id, this)
  val memoTable = Map[List[Any], ArrayBuffer[MemoEntry]]()
  val adjustableLists = Set[AdjustableList[Any, Any]]()

  private var task: Task = null
  val tbd = new TBD(id, this)

  var nextModId = 0

  // During change propagation, represents the number of child workers this
  // worker is waiting to receive FinishedPropagatingMessages from before it
  // can continue.
  var awaiting = 0

  def propagate(): Boolean = {
    awaiting += ddg.root.propagate(this)

    if (awaiting == 0) {
      tbd.updatedMods.clear()
    }

    return awaiting == 0
  }

  private def get(key: ModId): Any = {
    val ret = tbd.mods(key)

    if (ret == null) {
      NullMessage
    } else {
      ret
    }
  }

  def receive = {
    case GetMessage(table: String, key: ModId) => {
      if (tbd.mods(key) == null) {
        sender ! NullMessage
      } else {
        sender ! tbd.mods(key)
      }

      assert(table == "mods")
    }

    case ReadModMessage(modId: ModId, workerRef: ActorRef) => {
      sender ! get(modId)

      if (tbd.dependencies.contains(modId)) {
        tbd.dependencies(modId) += workerRef
      } else {
        tbd.dependencies(modId) = Set(workerRef)
      }
    }

    case ModUpdatedMessage(modId: ModId, respondTo: ActorRef) => {
      parent ! PebbleMessage(self, modId, respondTo)

      ddg.modUpdated(modId)
      tbd.updatedMods += modId
    }

    case RunTaskMessage(aTask: Task) => {
      task = aTask
      val output = task.func(tbd)

      sender ! output
    }

    case PebbleMessage(workerRef: ActorRef, modId: ModId, respondTo: ActorRef) => {
      val newPebble = ddg.parUpdated(workerRef)

      if (newPebble) {
        parent ! PebbleMessage(self, modId, respondTo)
      } else {
        respondTo ! PebblingFinishedMessage(modId)
      }
    }

    case PebblingFinishedMessage(modId: ModId) => {
      assert(tbd.awaiting > 0)
      tbd.awaiting -= 1

      if (!tbd.initialRun && tbd.awaiting == 0 && awaiting == 0) {
        parent ! FinishedPropagatingMessage
      }
    }

    case PropagateMessage => {
      tbd.initialRun = false
      val done = propagate()

      if (done && tbd.awaiting == 0) {
        parent ! FinishedPropagatingMessage
      }
    }

    case FinishedPropagatingMessage => {
      assert(awaiting > 0)
      awaiting -= 1

      if (awaiting == 0) {
        val done = propagate()

        if (done && tbd.awaiting == 0) {
          parent ! FinishedPropagatingMessage
        }
      }
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
