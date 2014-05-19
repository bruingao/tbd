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
import akka.pattern.ask
import scala.collection.mutable.{Map, Set}

import tbd.messages._
import tbd.mod._

object Datastore {
  def props(): Props = Props(classOf[Datastore])
}

class Datastore extends Actor with ActorLogging {
  val tables = Map[String, Map[Any, Any]]()
  tables("mods") = Map[Any, Any]()

  // Maps modIds to the workers that have read the corresponding mod.
  private val dependencies = Map[ModId, Set[ActorRef]]()

  // Maps the name of an input table to a set containing the Modifiers that were
  // returned containing elements from this table, so that we can inform them
  // when the table is updated.
  private val modifiers = Map[String, Set[Modifier[Any, Any]]]()

  private def get(table: String, key: Any): Any = {
    if (!tables(table).contains(key)) {
      log.warning("Trying to get nonexistent key = " + key + " from table = " +
                  table)
    }
    val ret = tables(table)(key)

    if (ret == null) {
      NullMessage
    } else {
      ret
    }
  }

  private var nextModId = 0
  def createMod[T](value: T): Mod[T] = {
    val mod = new Mod[T](self, new ModId("d." + nextModId))
    nextModId += 1

    tables("mods")(mod.id) = value
    dependencies(mod.id) = Set[ActorRef]()
    mod
  }

  def getMod(modId: ModId): Any = {
    tables("mods")(modId)
  }

  def updateMod(modId: ModId, value: Any, sender: ActorRef): Int = {
    if (!tables("mods").contains(modId)) {
      log.warning("Trying to update non-existent mod " + modId)
    }
    tables("mods")(modId) = value

    var count = 0
    for (workerRef <- dependencies(modId)) {
      if (workerRef != sender) {
        workerRef ! ModUpdatedMessage(modId, sender)
        count += 1
      }
    }

    count
  }

  def receive = {
    case CreateTableMessage(table: String) => {
      tables(table) = Map[Any, Any]()
    }

    case GetMessage(table: String, key: Any) => {
      sender ! get(table, key)
    }

    case PutMessage(table: String,
                    key: Any,
                    value: Any,
                    respondTo: ActorRef) => {
      if (tables(table).contains(key)) {
	log.warning("Putting input key that already exists.")
      }

      tables(table)(key) = value

      var count = 0
      if (modifiers.contains(table)) {
        for (modifier <- modifiers(table)) {
          count += modifier.insert(key, value, respondTo)
        }
      }

      sender ! count
    }

    case UpdateMessage(table: String,
                       key: Any,
                       value: Any,
                       respondTo: ActorRef) => {
      tables(table)(key) = value

      var count = 0
      if (modifiers.contains(table)) {
        for (modifier <- modifiers(table)) {
          count += modifier.update(key, value, respondTo)
        }
      }

      sender ! count
    }

    case RemoveMessage(table: String, key: Any, respondTo: ActorRef) => {
      if (!tables(table).contains(key)) {
	log.warning("Trying to remove non-existent key from table.")
      }

      var count = 0
      if (modifiers.contains(table)) {
        for (modifier <- modifiers(table)) {
          count += modifier.remove(key, respondTo)
        }
      }

      tables(table) -= key

      sender ! count
    }

    case CreateModMessage(value: Any) => {
      sender ! createMod(value)
    }

    case CreateModMessage(null) => {
      sender ! createMod(null)
    }

    case UpdateModMessage(modId: ModId, value: Any, workerRef: ActorRef) => {
      sender ! updateMod(modId, value, workerRef)
    }

    case UpdateModMessage(modId: ModId, null, workerRef: ActorRef) => {
      sender ! updateMod(modId, null, workerRef)
    }

    case ReadModMessage(modId: ModId, workerRef: ActorRef) => {
      if (!tables("mods").contains(modId)) {
        log.warning("Trying to read non-existent mod")
      }

      dependencies(modId) += workerRef
      sender ! get("mods", modId)
    }

    case CleanUpMessage(
        workerRef: ActorRef,
        removeLists: Set[AdjustableList[Any, Any]]) => {
      for ((modId, dependencySet) <- dependencies) {
        dependencySet -= workerRef
      }

      for (table <- modifiers.keys) {
        for (removeList <- removeLists) {
          modifiers(table) = modifiers(table)
                             .filter((modifier: Modifier[Any, Any]) => {
            removeList != modifier.getModifiable()
          })
        }
      }

      sender ! "done"
    }

    case GetModMessage(table: String, key: Any) => {
      val modifier = new ModModifier(this, key, tables(table)(key))

      if (modifiers.contains(table)) {
        modifiers(table) += modifier
      } else {
        modifiers(table) = Set(modifier)
      }

      sender ! modifier.getModifiable()
    }

    case GetAdjustableListMessage(
        table: String,
        partitions: Int,
        chunkSize: Int,
        chunkSizer: (Any => Int)) => {
      val modifier =
        if (chunkSize == 0) {
          if (partitions == 1) {
            new DMLModifier[Any, Any](this, tables(table))
          } else {
            new PDMLModifier[Any, Any](this, tables(table), partitions)
          }
        } else {
	  if (partitions == 1) {
            new ChunkListModifier[Any, Any](
              this,
              tables(table),
              chunkSize,
              chunkSizer)
	  } else {
	    new PCLModifier[Any, Any](
              this,
              tables(table),
	      partitions,
              chunkSize,
              chunkSizer)
	  }
        }

      if (modifiers.contains(table)) {
        modifiers(table) += modifier
      } else {
        modifiers(table) = Set(modifier)
      }

      sender ! modifier.getModifiable()
    }

    case x => {
      log.warning("Datastore actor received unhandled message " +
                  x + " from " + sender)
    }
  }
}
