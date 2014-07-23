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

import akka.actor.{Actor, ActorRef, ActorLogging, Props}
import scala.collection.mutable.{ArrayBuffer, Map, Set}
import scala.concurrent.{Future, Promise}

import tbd.Constants._
import tbd.{AdjustableConf, ListConf, TableConf}
import tbd.messages._
import tbd.mod._

object Datastore {
  def props(storeType: String, cacheSize: Int): Props =
    Props(classOf[Datastore], storeType, cacheSize)
}

class Datastore(storeType: String, cacheSize: Int) extends Actor with ActorLogging {
  import context.dispatcher

  val store =
    if (storeType == "memory") {
      new MemoryStore()
    } else if (storeType == "berkeleydb") {
      val dbActor = context.actorOf(Props[BerkeleyDBActor])
      new BerkeleyDBStore(cacheSize, dbActor)
    } else {
      println("WARNING: storeType '" + storeType + "' is invalid. Using " +
              "'memory' instead")
      new MemoryStore()
    }

  // Maps the name of an input table to a set containing the Modifiers that were
  // returned containing elements from this table, so that we can inform them
  // when the table is updated.
  private val inputs = Map[InputId, Modifier]()

  private val dependencies = Map[ModId, Set[ActorRef]]()

  private var nextInputId: InputId = 0

  private var nextModId = 0
  def createMod[T](value: T): Mod[T] = {
    val mod = new Mod[T](new ModId("d." + nextModId))
    nextModId += 1

    store.put(mod.id, value)

    mod
  }

  def getMod(modId: ModId, workerRef: ActorRef = null): Any = {
    if (workerRef != null) {
      if (dependencies.contains(modId)) {
	dependencies(modId) += workerRef
      } else {
	dependencies(modId) = Set(workerRef)
      }
    }

    store.get(modId)
  }

  def updateMod(modId: ModId, value: Any): ArrayBuffer[Future[String]] = {
    if (!store.contains(modId)) {
      log.warning("Trying to update non-existent mod " + modId)
    }

    store.put(modId, value)

    val futures = ArrayBuffer[Future[String]]()
    if (dependencies.contains(modId)) {
      for (workerRef <- dependencies(modId)) {
	val finished = Promise[String]()
	workerRef ! ModUpdatedMessage(modId, finished)
	futures += finished.future
      }
    }

    futures
  }

  def removeMod(modId: ModId) {
    if (!store.contains(modId)) {
      log.warning("Trying to remove nonexistent mod " + modId)
    }

    store.remove(modId)
  }

  def receive = {
    case CreateAdjustableMessage(conf: AdjustableConf) => {
      val inputId = nextInputId
      nextInputId += 1

      val modifier =
	conf match {
	  case conf: ListConf =>
	    if (conf.chunkSize > 1) {
	      if (conf.partitions == 1) {
		log.info("Creating new ChunkList.")
		new ChunkListModifier(this, conf)
	      } else {
		log.info("Creating new PartitionedChunkList.")
		new PartitionedChunkListModifier(this, conf)
	      }
	    } else {
	      if (conf.partitions == 1) {
		log.info("Creating new ModList.")
		new ModListModifier(this, conf)
	      } else {
		log.info("Creating new PartitionedModList.")
		new PartitionedModListModifier(this, conf)
	      }
	    }
	  case TableConf() => {
	    new TableModifier(this)
	  }
	}
      inputs(inputId) = modifier

      sender ! inputId
    }

    case PutInputMessage(inputId: InputId, key: Any, value: Any) => {
      val futures = inputs(inputId).insert(key, value)

      sender ! Future.sequence(futures)
    }

    case UpdateInputMessage(inputId: InputId, key: Any, value: Any) => {
      val futures = inputs(inputId).update(key, value)

      sender ! Future.sequence(futures)
    }

    case RemoveInputMessage(inputId: InputId, key: Any) => {
      val futures = inputs(inputId).remove(key)

      sender ! Future.sequence(futures)
    }

    case GetInputMessage(inputId: InputId) => {
      sender ! inputs(inputId).getModifiable()
    }

    case CreateModMessage() => {
      sender ! createMod(null)
    }

    case GetModMessage(modId: ModId, workerRef: ActorRef) => {
      val ret = getMod(modId, workerRef)

      if (ret == null) {
	sender ! NullMessage
      } else {
	sender ! ret
      }
    }

    case GetModMessage(modId: ModId, null) => {
      val ret = getMod(modId, null)

      if (ret == null) {
	sender ! NullMessage
      } else {
	sender ! ret
      }
    }

    case UpdateModMessage(modId: ModId, value: Any) => {
      sender ! updateMod(modId, value)
    }

    case UpdateModMessage(modId: ModId, null) => {
      sender ! updateMod(modId, null)
    }

    case CleanupMessage => {
      store.shutdown()
      sender ! "done"
    }

    case x => {
      log.warning("Datastore actor received unhandled message " +
                  x + " from " + sender)
    }
  }
}
