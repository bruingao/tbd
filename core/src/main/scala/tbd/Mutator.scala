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
package tbd

import akka.pattern.ask
import scala.concurrent.{Await, Future}

import tbd.Constants._
import tbd.ddg.DDG
import tbd.master.Main
import tbd.messages._
import tbd.mod.Mod

class Mutator(aMain: Main = null) {
  var launchedMain = false

  val main =
    if (aMain == null) {
      launchedMain = true
      new Main()
    } else {
      aMain
    }

  val idFuture = main.masterRef ? RegisterMutatorMessage
  val id = Await.result(idFuture, DURATION).asInstanceOf[Int]

  def run[T](adjust: Adjustable): T = {
    val future = main.masterRef ? RunMessage(adjust, id)
    val resultFuture =
      Await.result(future, DURATION).asInstanceOf[Future[Any]]

    Await.result(resultFuture, DURATION).asInstanceOf[T]
  }

  def propagate() {
    val future = main.masterRef ? PropagateMessage
    val future2 = Await.result(future, DURATION).asInstanceOf[Future[String]]
    Await.result(future2, DURATION)
  }

  def put(key: Any, value: Any) {
    val future = main.masterRef ? PutInputMessage("input", key, value)
    val future2 = Await.result(future, DURATION).asInstanceOf[Future[String]]
    Await.result(future2, DURATION)
  }

  def update(key: Any, value: Any) {
    val future = main.masterRef ? UpdateInputMessage("input", key, value)
    val future2 = Await.result(future, DURATION).asInstanceOf[Future[String]]
    Await.result(future2, DURATION)
  }

  def remove(key: Any) {
    val future = main.masterRef ? RemoveInputMessage("input", key)
    val future2 = Await.result(future, DURATION).asInstanceOf[Future[String]]
    Await.result(future2, DURATION)
  }

  def getDDG(): DDG  = {
    val ddgFuture = main.masterRef ? GetMutatorDDGMessage(id)
    Await.result(ddgFuture, DURATION).asInstanceOf[DDG]
  }

  def shutdown() {
    if (launchedMain) {
      main.shutdown()
    } else {
      Await.result(main.masterRef ? ShutdownMutatorMessage(id), DURATION)
    }
  }
}
