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
import akka.pattern.{ask, pipe}
import scala.collection.mutable.{ArrayBuffer, Map, MutableList, Set}
import scala.concurrent.{Await, Future, Promise}
import scala.util.{Failure, Success, Try}

import tbd.Constants._
import tbd.{Adjustable, Context}
import tbd.ddg._
import tbd.messages._

object Task {
  def props
      (taskId: TaskId,
       parent: ActorRef,
       datastore: ActorRef,
       masterRef: ActorRef): Props =
    Props(classOf[Task], taskId, parent, datastore, masterRef)
}

class Task
    (taskId: TaskId,
     parent: ActorRef,
     datastore: ActorRef,
     masterRef: ActorRef)
  extends Actor with ActorLogging {
  import context.dispatcher

  private val c = new Context(taskId, this, datastore, masterRef)

  def propagate(start: Timestamp = Timestamp.MIN_TIMESTAMP,
                end: Timestamp = Timestamp.MAX_TIMESTAMP): Future[Boolean] = {
    Future {
      var option = c.ddg.updated.find((timestamp: Timestamp) =>
        timestamp > start && timestamp < end)
      while (!option.isEmpty) {
        val timestamp = option.get
        val node = timestamp.node
        c.ddg.updated -= timestamp

        node match {
          case readNode: ReadNode =>
            if (readNode.updated) {
              val newValue = c.readId(readNode.modId)

              val oldStart = c.reexecutionStart
              c.reexecutionStart = timestamp.getNext()
              val oldEnd = c.reexecutionEnd
              c.reexecutionEnd = timestamp.end
              val oldCurrentModId = c.currentModId
              c.currentModId = readNode.currentModId
              val oldCurrentModId2 = c.currentModId2
              c.currentModId2 = readNode.currentModId2

              val oldCurrentTime = c.currentTime
              c.currentTime = timestamp

              readNode.updated = false
              readNode.reader(newValue)

              if (c.reexecutionStart < c.reexecutionEnd) {
                c.ddg.ordering.splice(c.reexecutionStart, c.reexecutionEnd, c)
              }

              c.reexecutionStart = oldStart
              c.reexecutionEnd = oldEnd
              c.currentModId = oldCurrentModId
              c.currentModId2 = oldCurrentModId2
              c.currentTime = oldCurrentTime
            }
          case readNode: Read2Node =>
            if (readNode.updated) {
              val newValue1 = c.readId(readNode.modId1)
              val newValue2 = c.readId(readNode.modId2)

              val oldStart = c.reexecutionStart
              c.reexecutionStart = timestamp.getNext()
              val oldEnd = c.reexecutionEnd
              c.reexecutionEnd = timestamp.end
              val oldCurrentModId = c.currentModId
              c.currentModId = readNode.currentModId
              val oldCurrentModId2 = c.currentModId2
              c.currentModId2 = readNode.currentModId2

              val oldCurrentTime = c.currentTime
              c.currentTime = timestamp

              readNode.updated = false
              readNode.reader(newValue1, newValue2)

              if (c.reexecutionStart < c.reexecutionEnd) {
                c.ddg.ordering.splice(c.reexecutionStart, c.reexecutionEnd, c)
              }

              c.reexecutionStart = oldStart
              c.reexecutionEnd = oldEnd
              c.currentModId = oldCurrentModId
              c.currentModId2 = oldCurrentModId2
              c.currentTime = oldCurrentTime
            }
          case parNode: ParNode =>
            if (parNode.updated) {
              Await.result(Future.sequence(c.pending), DURATION)
              c.pending.clear()

              val future1 = parNode.taskRef1 ? PropagateTaskMessage
              val future2 = parNode.taskRef2 ? PropagateTaskMessage

              parNode.pebble1 = false
              parNode.pebble2 = false

              Await.result(future1, DURATION)
              Await.result(future2, DURATION)
            }
        }

        option = c.ddg.updated.find((timestamp: Timestamp) =>
          timestamp > start && timestamp < end)
      }

      true
    }
  }

  def receive = {
    case ModUpdatedMessage(modId: ModId) =>
      c.ddg.modUpdated(modId)
      c.updatedMods += modId

      (parent ? PebbleMessage(self, modId)) pipeTo sender

    case RunTaskMessage(adjust: Adjustable[_]) =>
      log.debug("Starting task.")
      sender ! adjust.run(c)
      Await.result(Future.sequence(c.pending), DURATION)
      c.pending.clear()

    case PebbleMessage(taskRef: ActorRef, modId: ModId) =>
      val newPebble = c.ddg.parUpdated(taskRef)

      if (newPebble) {
        (parent ? PebbleMessage(self, modId)) pipeTo sender
      } else {
	sender ! "done"
      }

    case PropagateTaskMessage =>
      log.debug("Running change propagation.")
      c.initialRun = false
      c.epoch += 1

      val respondTo = sender
      val future = propagate()
      future onComplete {
	case Success(t) =>
	  c.updatedMods.clear()

          Await.result(Future.sequence(c.pending), DURATION)
          c.pending.clear()

          respondTo ! "done"
	case Failure(e) =>
	  e.printStackTrace()
      }

    case GetTaskDDGMessage =>
      sender ! c.ddg

    case ShutdownTaskMessage =>
      self ! akka.actor.PoisonPill

      val futures = Set[Future[Any]]()
      for ((actorRef, parNode) <- c.ddg.pars) {
        futures += actorRef ? ShutdownTaskMessage
      }

      datastore ? RemoveModsMessage(c.ddg.getMods())

      Future.sequence(futures) pipeTo sender

    case x =>
      log.warning("Received unhandled message " + x + " from " + sender)
  }
}
