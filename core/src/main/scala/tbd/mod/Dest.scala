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
package tbd.mod

import akka.actor.ActorRef
import akka.pattern.ask
import scala.concurrent.Await

import tbd.Constants._
import tbd.TBD
import tbd.master.Main
import tbd.messages.CreateModMessage
import tbd.worker.Worker

class Dest[T](worker: Worker, aModId: ModId) extends Mod[T](null, aModId) {
  val future = worker.datastoreRef ? CreateModMessage(null)
  val mod = Await.result(future, DURATION).asInstanceOf[Mod[T]]

  worker.nextModId += 1

  override def read(workerRef: ActorRef = null): T = ???

  override def update(value: T, workerRef: ActorRef, tbd: TBD): Int = ???
}
