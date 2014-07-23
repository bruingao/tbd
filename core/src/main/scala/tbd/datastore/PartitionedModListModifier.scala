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

import scala.collection.mutable.{ArrayBuffer, Map}
import scala.concurrent.Future

import tbd.ListConf
import tbd.mod._

class PartitionedModListModifier(
    _datastore: Datastore,
    conf: ListConf) extends Modifier(_datastore) {

  val partitionModifiers = ArrayBuffer[ModListModifier]()
  val modList = initialize()

  private def initialize(): PartitionedModList[Any, Any] = {
    val partitions = new ArrayBuffer[ModList[Any, Any]]()
    for (i <- 1 to conf.partitions) {
      val modListModifier = new ModListModifier(datastore, conf)
      partitionModifiers += modListModifier
      partitions += modListModifier.modList
    }

    new PartitionedModList[Any, Any](partitions)
  }

  private var insertInto = 0
  def insert(key: Any, value: Any): ArrayBuffer[Future[String]] = {
    insertInto = (insertInto + 1) % conf.partitions
    partitionModifiers(insertInto).insert(key, value)
  }

  def update(key: Any, value: Any): ArrayBuffer[Future[String]] = {
    var futures = ArrayBuffer[Future[String]]()

    var found = false
    for (partitionModifier <- partitionModifiers) {
      if (!found && partitionModifier.contains(key)) {
        futures = partitionModifier.update(key, value)
        found = true
      }
    }

   if (!found) {
     println("Warning: tried to update nonexistant key")
   }

    futures
  }

  def remove(key: Any): ArrayBuffer[Future[String]] = {
    var futures = ArrayBuffer[Future[String]]()

    var found = false
    for (partitionModifier <- partitionModifiers) {
      if (!found && partitionModifier.contains(key)) {
        futures = partitionModifier.remove(key)
        found = true
      }
    }

    futures
  }

  def getModifiable(): AdjustableList[Any, Any] = {
    modList
  }
}
