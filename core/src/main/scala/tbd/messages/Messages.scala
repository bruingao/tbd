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
import scala.concurrent.Promise

import tbd.{Adjustable, AdjustableConf, Changeable, TBD}
import tbd.Constants._
import tbd.ddg.Node
import tbd.mod.{AdjustableList, Mod}

// Datastore
case class CreateAdjustableMessage(conf: AdjustableConf)
case class PutInputMessage(inputId: InputId, key: Any, value: Any)
case class UpdateInputMessage(inputId: InputId, key: Any, value: Any)
case class RemoveInputMessage(inputId: InputId, key: Any)
case class GetInputMessage(inputId: InputId)

case class CreateModMessage()
case class GetModMessage(modId: ModId, workerRef: ActorRef)
case class UpdateModMessage(modId: ModId, value: Any)
case class NullMessage()

// Master
case class RegisterMutatorMessage()
case class RunMessage(adjust: Adjustable[_], mutatorId: Int)
case class GetMutatorDDGMessage(mutatorId: Int)
case class ShutdownMutatorMessage(mutatorId: Int)
case class CleanupMessage()

// Worker
case class ModUpdatedMessage(modId: ModId, finished: Promise[String])
case class PebbleMessage(workerRef: ActorRef, modId: ModId, finished: Promise[String])
case class PropagateMessage()
case class RunTaskMessage(adjust: Adjustable[_])
case class GetDDGMessage()
case class DDGToStringMessage(prefix: String)
case class CleanupWorkerMessage()
