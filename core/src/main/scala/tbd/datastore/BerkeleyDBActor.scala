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

import akka.actor.{Actor, ActorLogging}
import com.sleepycat.je.{Environment, EnvironmentConfig}
import com.sleepycat.persist.{EntityStore, StoreConfig}
import com.sleepycat.persist.model.{Entity, PrimaryKey, SecondaryKey}
import com.sleepycat.persist.model.Relationship
import java.io._

import tbd.Constants._
import tbd.messages._

class BerkeleyDBActor extends Actor with ActorLogging{
  private var environment: Environment = null
  private var store: EntityStore = null

  private val envConfig = new EnvironmentConfig()
  envConfig.setAllowCreate(true)
  val storeConfig = new StoreConfig()
  storeConfig.setAllowCreate(true)

  val random = new scala.util.Random()
  private val envHome = new File("/tmp/tbd_berkeleydb" + random.nextInt())
  envHome.mkdir()

  try {
    // Open the environment and entity store
    environment = new Environment(envHome, envConfig)
    store = new EntityStore(environment, "EntityStore", storeConfig)
  } catch {
    case fnfe: FileNotFoundException => {
      System.err.println("setup(): " + fnfe.toString())
      System.exit(-1)
    }
  }

  val pIdx = store.getPrimaryIndex(classOf[ModId], classOf[ModEntity])

  var readCount = 0
  var writeCount = 0
  var deleteCount = 0

  def put(key: ModId, value: Any) {
    writeCount += 1
    val entity = new ModEntity()
    entity.key = key

    val byteOutput = new ByteArrayOutputStream()
    val objectOutput = new ObjectOutputStream(byteOutput)
    objectOutput.writeObject(value)
    entity.value = byteOutput.toByteArray

    pIdx.put(entity)
  }

  def receive = {
    case DBPutMessage(key: ModId, value: Any) => {
      put(key, value)
    }

    case DBPutMessage(key: ModId, null) => {
      put(key, null)
    }

    case DBGetMessage(key: ModId) => {
      readCount += 1
      val byteArray = pIdx.get(key).value

      val byteInput = new ByteArrayInputStream(byteArray)
      val objectInput = new ObjectInputStream(byteInput)
      val obj = objectInput.readObject()

      // No idea why this is necessary.
      if (obj != null && obj.isInstanceOf[Tuple2[_, _]]) {
	obj.toString
      }

      sender ! obj
    }

    case DBDeleteMessage(key: ModId) => {
      deleteCount += 1
      pIdx.delete(key)
    }

    case DBContainsMessage(key: ModId) => {
      sender ! pIdx.contains(key)
    }

    case DBShutdownMessage() => {
      log.info("Shutting down. writes = " + writeCount + ", reads = " +
	       readCount + ", deletes = " + deleteCount)
      store.close()
      environment.close()
    }

    case x => log.warning("unknown message received by BerkeleyDBActor " + x)
  }
}

@Entity
class ModEntity {
  @PrimaryKey
  var key: ModId = null

  var value: Array[Byte] = null
}
