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
package tbd.messages

import akka.actor.ActorRef
import scala.collection.mutable.{Map, Set}
import scala.language.existentials

import tbd.{Adjustable, Changeable, TBD}
import tbd.Constants._
import tbd.ddg.Node
import tbd.list.ListConf

// Datastore
case class CreateModMessage(value: Any)
case class GetModMessage(modId: ModId, task: ActorRef)
case class UpdateModMessage(modId: ModId, value: Any, task: ActorRef)
case class RemoveModsMessage(mods: Iterable[ModId])
case class NullMessage()
case class RegisterDatastoreMessage(workerId: WorkerId, datastoreRef: ActorRef)
case class SetIdMessage(workerId: WorkerId)
case class ClearMessage()

// Master
case class RegisterMutatorMessage()
case class RunMutatorMessage(adjust: Adjustable[_], mutatorId: Int)
case class PropagateMutatorMessage(mutatorId: Int)
case class GetMutatorDDGMessage(mutatorId: Int)
case class ScheduleTaskMessage(parent: ActorRef, workerId: WorkerId)
case class ShutdownMutatorMessage(mutatorId: Int)

case class CreateListMessage(conf: ListConf)
case class GetListsMessage(conf: ListConf)
case class GetListIdMessage(conf: ListConf)
case class GetAdjustableListMessage(listId: String)
case class PutMessage(listId: String, key: Any, value: Any)
case class UpdateMessage(listId: String, key: Any, value: Any)
case class RemoveMessage(listId: String, key: Any)
case class PutAfterMessage(listId: String, key: Any, newPair: (Any, Any))
case class LoadMessage(listId: String, data: Map[Any, Any])

// Worker
case class RegisterWorkerMessage(worker: ActorRef, datastoreRef: ActorRef)
case class CreateTaskMessage(parent: ActorRef)
case class GetDatastoreMessage()

// Task
case class ModUpdatedMessage(modId: ModId)
case class PebbleMessage(taskRef: ActorRef, modId: ModId)
case class PropagateTaskMessage()
case class RunTaskMessage(adjust: Adjustable[_])
case class GetTaskDDGMessage()
case class ShutdownTaskMessage()
