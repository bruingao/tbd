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

import com.sleepycat.je.{DatabaseConfig, DatabaseException, Environment, EnvironmentConfig}
import com.sleepycat.persist.{EntityStore, StoreConfig}
import com.sleepycat.persist.model.{Entity, PrimaryKey, SecondaryKey}
import com.sleepycat.persist.model.Relationship._
import java.io._
import scala.collection.mutable.Map

import tbd.Constants._
import tbd.mod.Mod

class LRUNode(
  val key: ModId,
  var value: Any,
  var previous: LRUNode,
  var next: LRUNode
)

class BerkeleyDBStore(cacheSize: Int) extends KVStore {
  private val values = Map[ModId, LRUNode]()
  private val tail = new LRUNode(null, null, null, null)
  private var head = tail

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

  def put(key: ModId, value: Any) {
    if (values.contains(key)) {
      values(key).value = value
    } else {
      val newNode = new LRUNode(key, value, null, head)
      values(key) = newNode

      head.previous = newNode
      head = newNode

      if (values.size > cacheSize) {
	evict()
      }
    }
  }

  private def evict() {
    while (values.size > cacheSize) {
      val toEvict = tail.previous
      putDB(toEvict.key, toEvict.value)
      values -= toEvict.key

      tail.previous = toEvict.previous
      toEvict.previous.next = tail
    }
  }

  private def putDB(key: ModId, value: Any) {
    val entity = new ModEntity()
    entity.key = key

    val byteOutput = new ByteArrayOutputStream()
    val objectOutput = new ObjectOutputStream(byteOutput)
    objectOutput.writeObject(value)
    entity.value = byteOutput.toByteArray

    pIdx.put(entity)
  }

  def get(key: ModId): Any = {
    if (values.contains(key)) {
      values(key).value
    } else {
      assert(pIdx.contains(key))
      getDB(key)
    }
  }

  private def getDB(key: ModId): Any = {
    val byteArray = pIdx.get(key).value

    val byteInput = new ByteArrayInputStream(byteArray)
    val objectInput = new ObjectInputStream(byteInput)
    val obj = objectInput.readObject()

    // No idea why this is necessary.
    if (obj != null && obj.isInstanceOf[Tuple2[_, _]]) {
      obj.toString
    }

    obj
  }

  def remove(key: ModId) {
    if (values.contains(key)) {
      values -= key
    }

    pIdx.delete(key)
  }

  def contains(key: ModId): Boolean = {
    values.contains(key) || pIdx.contains(key)
  }

  def shutdown() {
    store.close()
    environment.close()
  }
}

@Entity
class ModEntity {
  @PrimaryKey
  var key: ModId = null

  var value: Array[Byte] = null
}
