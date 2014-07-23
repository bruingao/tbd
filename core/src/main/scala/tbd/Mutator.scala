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

  def run[T](adjust: Adjustable[T]): T = {
    val future = main.masterRef ? RunMessage(adjust, id)
    val resultFuture =
      Await.result(future, DURATION).asInstanceOf[Future[Any]]

    Await.result(resultFuture, DURATION).asInstanceOf[T]
  }

  def propagate() {
    val future = main.masterRef ? PropagateMessage
    Await.result(future, DURATION)
  }

  def createList[T, U](conf: ListConf = new ListConf()): ListInput[T, U] = {
    new ListInput[T, U](main.masterRef, conf)
  }

  def createChunkList[T, U](conf: ListConf = new ListConf())
      : ChunkListInput[T, U] = {
    new ChunkListInput[T, U](main.masterRef, conf)
  }

  def createTable[T, U](conf: TableConf = new TableConf()): TableInput[T, U] = {
    new TableInput[T, U](main.masterRef, conf)
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
