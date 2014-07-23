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
import java.io.Serializable
import scala.collection.mutable.{ArrayBuffer, Set}
import scala.concurrent.{Await, Future, Lock, Promise}

import tbd.Constants._
import tbd.TBD
import tbd.master.Main
import tbd.messages._

class Mod[T](val id: ModId) extends Serializable {

  def read(workerRef: ActorRef = null): T = {
    val valueFuture = Main.datastoreRef ? GetModMessage(id, workerRef)
    val ret = Await.result(valueFuture, DURATION)

    ret match {
      case NullMessage => null.asInstanceOf[T]
      case _ => ret.asInstanceOf[T]
    }
  }

  def update(_value: T): ArrayBuffer[Future[String]] = {
    val futuresFuture = Main.datastoreRef ? UpdateModMessage(id, _value)
    Await.result(futuresFuture.mapTo[ArrayBuffer[Future[String]]], DURATION)
  }

  override def equals(obj: Any): Boolean = {
    if (!obj.isInstanceOf[Mod[T]]) {
      false
    } else {
      val that = obj.asInstanceOf[Mod[T]]
      that.id == id
    }
  }

  override def toString = "Mod(" + id + ")"
}
