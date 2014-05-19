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
import scala.collection.mutable.Set

import tbd.{Adjustable, Changeable, TBD}
import tbd.ddg.Node
import tbd.mod.{AdjustableList, Mod, ModId}
import tbd.worker.Task

// DDG
case class AddReadMessage(modId: ModId, parent: Node)
case class AddWriteMessage(modId: ModId, parent: Node)
case class ToStringMessage()

// Datastore
case class CreateTableMessage(table: String)
case class GetMessage(table: String, key: Any)
case class PutMessage(table: String, key: Any, value: Any, respondTo: ActorRef)
case class UpdateMessage(
    table: String,
    key: Any,
    value: Any,
    respondTo: ActorRef)
case class RemoveMessage(table: String, key: Any, respondTo: ActorRef)

case class CreateModMessage(value: Any)
case class UpdateModMessage(modId: ModId, value: Any, workerRef: ActorRef)
case class ReadModMessage(modId: ModId, workerRef: ActorRef)
case class CleanUpMessage(
    workerRef: ActorRef,
    adjustableLists: Set[AdjustableList[Any, Any]])

case class GetModMessage(table: String, key: Any)
case class GetAdjustableListMessage(
    table: String,
    partitions: Int = 8,
    chunkSize: Int = 0,
    chunkSizer: Any => Int)
case class NullMessage()

// Master
case class RunMessage(adjust: Adjustable, mutatorId: Int)
case class PutInputMessage(table: String, key: Any, value: Any)
case class UpdateInputMessage(table: String, key: Any, value: Any)
case class RemoveInputMessage(table: String, key: Any)
case class RegisterMutatorMessage()
case class GetMutatorDDGMessage(mutatorId: Int)
case class ShutdownMutatorMessage(mutatorId: Int)

// Worker
case class ModUpdatedMessage(modId: ModId, respondTo: ActorRef)
case class PebbleMessage(workerRef: ActorRef, modId: ModId, respondTo: ActorRef)
case class PebblingFinishedMessage(modId: ModId)
case class PropagateMessage()
case class RunTaskMessage(func: Task)
case class FinishedPropagatingMessage()
case class GetDDGMessage()
case class DDGToStringMessage(prefix: String)
case class CleanupWorkerMessage()
