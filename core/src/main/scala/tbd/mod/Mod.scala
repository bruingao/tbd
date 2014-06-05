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
import scala.collection.mutable.{ArrayBuffer, Set}
import scala.concurrent.{Await, Future, Lock, Promise}

import tbd.Constants._
import tbd.TBD
import tbd.messages._

class Mod[T](_id: ModId, _value: T) {
  val id = _id
  var value = _value
  val dependencies = Set[ActorRef]()
  val lock = new Lock()
  var replacedBy: Mod[_] = null

  def read(workerRef: ActorRef = null): T = {
    if (workerRef != null) {
      lock.acquire()
      dependencies += workerRef
      lock.release()
    }

    value
  }

  def update(_value: T, _replacedBy: Mod[_] = null): ArrayBuffer[Future[String]] = {
    value = _value
    replacedBy = _replacedBy

    val futures = ArrayBuffer[Future[String]]()
    lock.acquire()
    for (workerRef <- dependencies) {
      val finished = Promise[String]()
      workerRef ! ModUpdatedMessage(id, finished)
      futures += finished.future
    }
    lock.release()

    futures
  }

  override def toString = {
    if (value == null) {
      "null"
    } else {
      value.toString
    }
  }
}
