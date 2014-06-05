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
import scala.concurrent.Future

import tbd.Constants._
import tbd.messages._
import tbd.mod._

object Datastore {
  def props(): Props = Props(classOf[Datastore])
}

class Datastore extends Actor with ActorLogging {
  import context.dispatcher
  private val tables = Map[String, Map[Any, Any]]()
  tables("mods") = Map[Any, Any]()

  // Maps the name of an input table to a set containing the Modifiers that were
  // returned containing elements from this table, so that we can inform them
  // when the table is updated.
  private val modifiers = Map[String, Set[Modifier[Any, Any]]]()

  private def get(table: String, key: Any): Any = {
    if (!tables(table).contains(key)) {
      log.warning("Trying to get nonexistent key = " + key + " from table = " +
                  table)
      NullMessage
    } else {
      val ret = tables(table)(key)

      if (ret == null) {
	NullMessage
      } else {
	ret
      }
    }
  }

  private var nextModId = 0
  def createMod[T](value: T): Mod[T] = {
    val mod = new Mod[T](new ModId("d." + nextModId), value)
    nextModId += 1

    log.debug("Creating modId=" + mod.id + " value=" + value)
    tables("mods")(mod.id) = mod
    mod
  }

  def getMod(modId: ModId): Any = {
    tables("mods")(modId).asInstanceOf[Mod[Any]].value
  }

  def updateMod(modId: ModId, value: Any, replacedBy: Mod[_] = null)
      : ArrayBuffer[Future[String]] = {
    if (!tables("mods").contains(modId)) {
      log.warning("Trying to update non-existent mod " + modId)
    }

    log.debug("Updating " + modId + " from " + tables("mods")(modId) + " to " +
	      value + " replaced by " + replacedBy)
    if (replacedBy != null) {
      log.debug("replaced by id = " + replacedBy.id)
    }
    tables("mods")(modId).asInstanceOf[Mod[Any]].update(value, replacedBy)
  }

  def removeMod(modId: ModId) {
    if (!tables("mods").contains(modId)) {
      log.warning("Trying to remove nonexistent mod " + modId)
    }
    log.debug("Removing modId=" + modId + " value=" + tables("mods")(modId))
    tables("mods")(modId).asInstanceOf[Mod[Any]].replacedBy = null
    tables("mods") -= modId
  }

  def receive = {
    case CreateTableMessage(table: String) => {
      tables(table) = Map[Any, Any]()
    }

    case GetMessage(table: String, key: Any) => {
      sender ! get(table, key)
    }

    case PutMessage(table: String, key: Any, value: Any) => {
      if (tables(table).contains(key)) {
	log.warning("Putting input key that already exists.")
      }

      tables(table)(key) = value

      val futures = ArrayBuffer[Future[String]]()
      if (modifiers.contains(table)) {
        for (modifier <- modifiers(table)) {
          futures ++= modifier.insert(key, value)
        }
      }

      sender ! Future.sequence(futures)
    }

    case UpdateMessage(table: String, key: Any, value: Any) => {
      tables(table)(key) = value

      val futures = ArrayBuffer[Future[String]]()
      if (modifiers.contains(table)) {
        for (modifier <- modifiers(table)) {
          futures ++= modifier.update(key, value)
        }
      }

      sender ! Future.sequence(futures)
    }

    case RemoveMessage(table: String, key: Any) => {
      if (!tables(table).contains(key)) {
	log.warning("Trying to remove non-existent key from table.")
      }

      val futures = ArrayBuffer[Future[String]]()
      if (modifiers.contains(table)) {
        for (modifier <- modifiers(table)) {
          futures ++= modifier.remove(key)
        }
      }

      tables(table) -= key

      sender ! Future.sequence(futures)
    }

    case CleanUpMessage(
        workerRef: ActorRef,
        removeLists: Set[AdjustableList[Any, Any]]) => {
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
        chunkSizer: (Any => Int),
        valueMod: Boolean) => {
      val modifier =
	if (!valueMod) {
          if (chunkSize == 0) {
	    if (partitions == 1) {
	      log.info("Creating new ModList.")
	      new ModListModifier[Any, Any](this, tables(table))
	    } else {
	      log.info("Creating new PartitionedModList.")
	      new PartitionedModListModifier[Any, Any](this, tables(table), partitions)
	    }
          } else {
            if (partitions == 1) {
              log.info("Creating new ChunkList.")
              new ChunkListModifier[Any, Any](this, tables(table), chunkSize,
                                              chunkSizer)
            } else {
              log.info("Creating new PartitionedChunkList.")
              new PartitionedChunkListModifier[Any, Any](this, tables(table), partitions,
                                                         chunkSize, chunkSizer)
            }
          }
	} else {
          if (chunkSize == 0) {
            if (partitions == 1) {
	      log.info("Creating new DoubleModList.")
              new DMLModifier[Any, Any](this, tables(table))
            } else {
	      log.info("Creating new PartitionedDoubleModList.")
              new PDMLModifier[Any, Any](this, tables(table), partitions)
            }
          } else {
	    if (partitions == 1) {
	      log.info("Creating new DoubleChunkList.")
              new DoubleChunkListModifier[Any, Any](
                this,
                tables(table),
                chunkSize,
                chunkSizer)
	    } else {
	      log.info("Creating new PartitionedDoubleChunkList.")
	      new PartitionedDoubleChunkListModifier[Any, Any](
                this,
                tables(table),
	        partitions,
                chunkSize,
                chunkSizer)
	    }
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
