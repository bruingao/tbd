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
import tbd.messages._

class Mod[T](datastoreRef: ActorRef, aId: ModId) {
  val id = aId

  def read(workerRef: ActorRef = null): T = {
    val ret =
      if (workerRef != null) {
        val readFuture = datastoreRef ? ReadModMessage(id, workerRef)
        Await.result(readFuture, DURATION)
      } else {
        val readFuture = datastoreRef ? GetMessage("mods", id)
        Await.result(readFuture, DURATION)
      }

    ret match {
      case NullMessage => null.asInstanceOf[T]
      case _ => ret.asInstanceOf[T]
    }
  }

  def update(value: T, workerRef: ActorRef, tbd: TBD): Int = {
    val future = datastoreRef ? UpdateModMessage(id, value, workerRef)
    Await.result(future, DURATION).asInstanceOf[Int]
  }

  override def toString = {
    val value = read()
    if (value == null) {
      "null"
    } else {
      value.toString
    }
  }
}
