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
package tbd.master

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.pattern.{ask, pipe}
import scala.collection.mutable.{ArrayBuffer, Map}
import scala.concurrent.{Await, Future, Promise}
import scala.util.{Failure, Success, Try}

import tbd.{Adjustable, AdjustableConf, TBD}
import tbd.Constants._
import tbd.datastore.Datastore
import tbd.messages._
import tbd.mod.{AdjustableChunkList, AdjustableList}
import tbd.worker.Worker

object Master {
  def props(datastoreRef: ActorRef): Props =
    Props(classOf[Master], datastoreRef)

  var epoch = 0
}

class Master(datastoreRef: ActorRef) extends Actor with ActorLogging {
  import context.dispatcher
  log.info("Master launced.")

  private var workerRef: ActorRef = null

  private var nextMutatorId = 0

  // Maps mutatorIds to their corresponding workers.
  private val workers = Map[Int, ActorRef]()

  def receive = {
    case RunMessage(adjust: Adjustable[_], mutatorId: Int) => {
      log.debug("RunMessage")

      val workerProps = Worker.props("w0", datastoreRef, self)
      workerRef = context.actorOf(workerProps, "worker" + mutatorId)
      workers(mutatorId) = workerRef

      val resultFuture = workerRef ? RunTaskMessage(adjust)

      sender ! resultFuture
    }

    case PropagateMessage => {
      log.info("Master actor initiating change propagation.")

      Master.epoch += 1
      val future = workerRef ? PropagateMessage
      val respondTo = sender
      future.onComplete((_try: Try[Any]) => {
	//log.debug("DDG: {}\n\n", Await.result(workerRef ? DDGToStringMessage(""),
        //                                      DURATION).asInstanceOf[String])
        respondTo ! "done"
      })
    }

    case CreateAdjustableMessage(conf: AdjustableConf) => {
      log.debug("CreateListMessage")
      val future = (datastoreRef ? CreateAdjustableMessage(conf))
	.mapTo[InputId]
      val respondTo = sender
      future.onComplete((_try: Try[InputId]) =>
	_try match {
	  case Success(inputId) => {
	    respondTo ! inputId
	  }
	  case Failure(error) =>
	    log.warning("PutMessage failed.")
	})
    }

    case PutInputMessage(inputId: InputId, key: Any, value: Any) => {
      log.debug("PutInputMessage")
      val future = (datastoreRef ? PutInputMessage(inputId, key, value))
	.mapTo[Future[ArrayBuffer[String]]]
      val respondTo = sender
      future.onComplete((_try: Try[Future[ArrayBuffer[String]]]) =>
	_try match {
	  case Success(future) => {
	    future.onComplete((_try: Try[ArrayBuffer[String]]) =>
              respondTo ! "done")
	  }
	  case Failure(error) =>
	    log.warning("PutMessage failed.")
	})
    }

    case UpdateInputMessage(inputId: InputId, key: Any, value: Any) => {
      log.debug("UpdateInputMessage")
      val future = (datastoreRef ? UpdateInputMessage(inputId, key, value))
	.mapTo[Future[ArrayBuffer[String]]]
      val respondTo = sender
      future.onComplete((_try: Try[Future[ArrayBuffer[String]]]) =>
	_try match {
	  case Success(future) => {
	    future.onComplete((_try: Try[ArrayBuffer[String]]) =>
              respondTo ! "done")
	  }
	  case Failure(error) =>
	    log.warning("UpdateMessage failed.")
	})
    }

    case RemoveInputMessage(inputId: InputId, key: Any) => {
      log.debug("RemoveInputMessage")
      val future = (datastoreRef ? RemoveInputMessage(inputId, key))
	.mapTo[Future[ArrayBuffer[String]]]
      val respondTo = sender
      future.onComplete((_try: Try[Future[ArrayBuffer[String]]]) =>
	_try match {
	  case Success(future) => {
	    future.onComplete((_try: Try[ArrayBuffer[String]]) =>
              respondTo ! "done")
	  }
	  case Failure(error) =>
	    log.warning("RemoveMessage failed.")
	})
    }

    case GetInputMessage(inputId: InputId) => {
      val future = (datastoreRef ? GetInputMessage(inputId))
      val respondTo = sender
      future.onComplete((_try: Try[Any]) =>
	_try match {
	  case Success(input) => {
	    respondTo ! input
	  }
	  case Failure(error) =>
	    log.warning("GetInputMessage failed. " + error)
	})
    }

    case PebbleMessage(workerRef: ActorRef, modId: ModId, finished: Promise[String]) => {
      finished.success("done")
    }

    case RegisterMutatorMessage => {
      sender ! nextMutatorId
      nextMutatorId += 1
    }

    case GetMutatorDDGMessage(mutatorId: Int) => {
      if (workers.contains(mutatorId)) {
        val future = workers(mutatorId) ? GetDDGMessage
        sender ! Await.result(future, DURATION)
      }
    }

    case ShutdownMutatorMessage(mutatorId: Int) => {
      if (workers.contains(mutatorId)) {
        log.debug("Sending CleanupWorkerMessage to " + workers(mutatorId))
        val future = workers(mutatorId) ? CleanupWorkerMessage
        Await.result(future, DURATION)

        context.stop(workers(mutatorId))
        workers -= mutatorId
      }

      sender ! "done"
    }

    case CleanupMessage => {
      Await.result((datastoreRef ? CleanupMessage), DURATION)
      sender ! "done"
    }

    case x => {
      log.warning("Master actor received unhandled message " +
		  x + " from " + sender + " " + x.getClass)
    }
  }
}
