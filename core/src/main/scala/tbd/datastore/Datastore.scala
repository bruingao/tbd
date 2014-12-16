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

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.pattern.ask
import java.io.{BufferedReader, File, FileReader}
import java.util.regex.Pattern
import scala.collection.mutable.{Buffer, Map}
import scala.concurrent.{Await, Future}

import tbd.Mod
import tbd.Constants._
import tbd.list._
import tbd.messages._

object Datastore {
  def props(storeType: String, cacheSize: Int, data: String): Props =
    Props(classOf[Datastore], storeType, cacheSize, data)
}

class Datastore
    (storeType: String,
     cacheSize: Int,
     data: String) extends Actor with ActorLogging {
  import context.dispatcher

  var workerId: WorkerId = _

  private val store = storeType match {
    case "memory" => new MemoryStore()
    case "berkeleydb" => new BerkeleyDBStore(cacheSize, context)
  }

  private var nextModId = 0

  private val lists = Map[String, ListInput[Any, Any]]()

  private var nextListId = 0

  private val dependencies = Map[ModId, Set[ActorRef]]()

  // Maps logical names of datastores to their references.
  private val datastores = Map[WorkerId, ActorRef]()

  private var misses = 0

  private val datafile = data

  def createMod[T](value: T): Mod[T] = {
    var newModId: Long = workerId
    newModId = newModId << 48
    newModId += nextModId

    nextModId += 1

    store.put(newModId, value)

    new Mod(newModId)
  }

  def read[T](mod: Mod[T]): T = {
    store.get(mod.id).asInstanceOf[T]
  }

  def getMod(modId: ModId, taskRef: ActorRef): Any = {
    if (store.contains(modId)) {
      val value = store.get(modId)
      if (value == null)
        NullMessage
      else
        value
    } else {
      val workerId = getWorkerId(modId)
      val future = datastores(workerId) ? GetModMessage(modId, taskRef)

      /*misses += 1
      log.info(misses + " misses")*/

      val result = Await.result(future, DURATION)

      if (result.isInstanceOf[Tuple2[_, _]]) {
        result.toString
      }

      result
    }
  }

  def update[T](mod: Mod[T], value: T) {
    val futures = Buffer[Future[String]]()

    if (!store.contains(mod.id) || store.get(mod.id) != value) {
      store.put(mod.id, value)

      if (dependencies.contains(mod.id)) {
        for (taskRef <- dependencies(mod.id)) {
          futures += (taskRef ? ModUpdatedMessage(mod.id)).mapTo[String]
        }
      }
    }

    Await.result(Future.sequence(futures), DURATION)
  }

  def updateMod
      (modId: ModId,
       value: Any,
       task: ActorRef,
       respondTo: ActorRef) {
    val futures = Buffer[Future[String]]()

    if (!store.contains(modId) || store.get(modId) != value) {
      store.put(modId, value)

      if (dependencies.contains(modId)) {
        for (taskRef <- dependencies(modId)) {
          if (taskRef != task) {
            futures += (taskRef ? ModUpdatedMessage(modId)).mapTo[String]
          }
        }
      }
    }

    Future.sequence(futures).onComplete {
      case value => respondTo ! "done"
    }
  }

  def receive = {
    case CreateModMessage(value: Any) =>
      sender ! createMod(value)

    case CreateModMessage(null) =>
      sender ! createMod(null)

    case GetModMessage(modId: ModId, taskRef: ActorRef) =>
      sender ! getMod(modId, taskRef)

      if (dependencies.contains(modId)) {
        dependencies(modId) += taskRef
      } else {
        dependencies(modId) = Set(taskRef)
      }

    case GetModMessage(modId: ModId, null) =>
      sender ! getMod(modId, null)

    case UpdateModMessage(modId: ModId, value: Any, task: ActorRef) =>
      updateMod(modId, value, task, sender)

    case UpdateModMessage(modId: ModId, value: Any, null) =>
      updateMod(modId, value, null, sender)

    case UpdateModMessage(modId: ModId, null, task: ActorRef) =>
      updateMod(modId, null, task, sender)

    case UpdateModMessage(modId: ModId, null, null) =>
      updateMod(modId, null, null, sender)

    case RemoveModsMessage(modIds: Iterable[ModId]) =>
      for (modId <- modIds) {
        store.remove(modId)
        dependencies -= modId
      }

      sender ! "done"

    case CreateListMessage(conf: ListConf) =>
      val listId = nextListId + ""
      nextListId += 1
      val list =
        if (conf.double) {
          new DoubleListModifier[Any, Any](this)
        } else if (conf.chunkSize == 1) {
          new ListModifier[Any, Any](this)
        } else {
          new ChunkListModifier[Any, Any](this, conf)
        }

      lists(listId) = list

      if (conf.file != "") {
        val file = new File(conf.file)
        val fileSize = file.length()

        val in = new BufferedReader(new FileReader(conf.file))
        val partitionSize = fileSize / conf.partitions
        var buf = new Array[Char](partitionSize.toInt)

        in.skip(partitionSize * conf.partitionIndex)
        in.read(buf)

        log.debug("Reading " + conf.file + " from " +
          (partitionSize * conf.partitionIndex) + " length " +
           partitionSize)

        val regex =
          Pattern.compile("""(?s)<key>(.*?)</key>[\s]*?<value>(.*?)</value>""")
        val str = new String(buf)
        val matcher = regex.matcher(str)

        var end = 0
        while (matcher.find()) {
          list.put(matcher.group(1), matcher.group(2))
          end = matcher.end()
        }

        if (conf.partitionIndex != conf.partitions - 1) {
          var remaining = str.substring(end)
          var done = false
          while (!done) {
            val smallBuf = new Array[Char](64)
            in.read(smallBuf)

            remaining += new String(smallBuf)
            val matcher = regex.matcher(remaining)
            if (matcher.find()) {
              list.put(matcher.group(1), matcher.group(2))
              done = true
            }
          }
        }
      }

      if (datafile != "") {
        val file = new File(datafile)
        val fileSize = file.length()
        val in = new BufferedReader(new FileReader(datafile))
        var buf = new Array[Char](fileSize.toInt)
        in.read(buf)

        val regex =
          Pattern.compile("""(?s)<key>(.*?)</key>[\s]*?<value>(.*?)</value>""")
        val str = new String(buf)
        val matcher = regex.matcher(str)
        while (matcher.find()) {
          list.put(matcher.group(1).toInt, matcher.group(2).toInt)
        }
      }

      sender ! listId

    case GetAdjustableListMessage(listId: String) =>
      sender ! lists(listId).getAdjustableList()

    case LoadMessage(listId: String, data: Map[Any, Any]) =>
      lists(listId).load(data)
      sender ! "okay"

    case PutMessage(listId: String, key: Any, value: Any) =>
      lists(listId).put(key, value)
      sender ! "okay"

    case UpdateMessage(listId: String, key: Any, value: Any) =>
      lists(listId).update(key, value)
      sender ! "okay"

    case RemoveMessage(listId: String, key: Any) =>
      lists(listId).remove(key)
      sender ! "okay"

    case PutAfterMessage(listId: String, key: Any, newPair: (Any, Any)) =>
      lists(listId).putAfter(key, newPair)
      sender ! "okay"

    case RegisterDatastoreMessage(workerId: WorkerId, datastoreRef: ActorRef) =>
      datastores(workerId) = datastoreRef

    case SetIdMessage(_workerId: WorkerId) =>
      workerId = _workerId

    case ClearMessage() =>
      lists.clear()

    case x =>
      log.warning("Datastore actor received unhandled message " +
                  x + " from " + sender)
  }
}
