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
import akka.actor.{ActorRef, ActorSystem, Props}
import akka.event.{Logging, LoggingAdapter}
import akka.util.Timeout
import scala.concurrent.{Await, Promise}
import scala.concurrent.duration._

import tbd.ddg.{DDG, Node}
import tbd.messages._
import tbd.mod.{Mod, ModId}
import tbd.worker.{Worker, Task}

object TBD {
  var id = 0
}

class TBD(
    id: String,
    ddg: DDG,
    datastoreRef: ActorRef,
    workerRef: ActorRef,
    system: ActorSystem,
    initialRun: Boolean) {

  private var currentReader: Node = null
  val input = new Reader(datastoreRef)

  val log = Logging(system, "TBD")

  implicit val timeout = Timeout(5 seconds)

  def read[T](mod: Mod[T], reader: (T) => (Changeable[T])): Changeable[T] = {
    log.debug("Executing read on  mod " + mod.id)

    val readNode = ddg.addRead(mod.id, currentReader, reader)
    log.debug("Contents of DDG after adding read:\n" + ddg)

    val outerReader = currentReader
    currentReader = readNode
    val changeable = reader(mod.read(workerRef))
    currentReader = outerReader

    ddg.addWrite(changeable.mod.id, readNode)

    changeable
  }

  def write[T](dest: Dest[T], value: T): Changeable[T] = {
    log.debug("Writing " + value + " to " + dest.mod.id)
    val future = datastoreRef ? UpdateModMessage(dest.mod.id, value)
    Await.result(future, timeout.duration)
    new Changeable(dest.mod)
  }

  def mod[T](initializer: Dest[T] => Changeable[T]): Mod[T] = {
    initializer(new Dest(datastoreRef)).mod
  }

  var workerId = 0
  def par[T, U](one: TBD => T, two: TBD => U): Tuple2[T, U] = {
    log.debug("Executing par.")
    val task1 =  new Task(((tbd: TBD) => one(tbd)))
    val workerProps1 =
      Worker.props[U](id + "-" + workerId, datastoreRef, workerRef)
    val workerRef1 = system.actorOf(workerProps1, id + "-" + workerId)
    workerId += 1
    val oneFuture = workerRef1 ? RunTaskMessage(task1)

    val task2 =  new Task(((tbd: TBD) => two(tbd)))
    val workerProps2 =
      Worker.props[U](id + "-" + workerId, datastoreRef, workerRef)
    val workerRef2 = system.actorOf(workerProps2, id + "-" +workerId)
    workerId += 1
    val twoFuture = workerRef2 ? RunTaskMessage(task2)

    ddg.addPar(workerRef1, workerRef2, currentReader)
    log.debug("DDG after adding par node:\n" + ddg)

    val oneRet = Await.result(oneFuture, timeout.duration).asInstanceOf[T]
    val twoRet = Await.result(twoFuture, timeout.duration).asInstanceOf[U]
    new Tuple2(oneRet, twoRet)
  }

  /*var memoId = 0
  def memo[T, U](): (List[Mod[T]]) => (() => Changeable[U]) => Changeable[U] = {
    implicit val timeout = Timeout(5 seconds)
    val thisMemoId = memoId
    memoId += 1
    (args: List[Mod[T]]) => {
      (func: () => Changeable[U]) => {
	if (initialRun) {
	  runMemo(args, func, thisMemoId)
	} else {
	  val updatedFuture = datastoreRef ? GetUpdatedMessage
	  val updatedMods = Await.result(updatedFuture, timeout.duration)
	    .asInstanceOf[Set[ModId]]

	  var updated = false
	  for (arg <- args) {
	    if (updatedMods.contains(arg.id)) {
	      updated = true
	    }
	  }

	  if (updated) {
	    log.debug("Did not find memoized value for call to " + thisMemoId)
	    runMemo(args, func, thisMemoId)
	  } else {
	    val memoFuture = datastoreRef ?
	      GetMessage("memo", thisMemoId :: args)
	    val memo = Await.result(memoFuture, timeout.duration)
	      .asInstanceOf[Changeable[U]]
	    log.debug("Found memoized value for call to " + thisMemoId)
	    memo
	  }
	}
      }
    }
  }

  private def runMemo[T, U](
      args: List[Mod[T]],
      func: () => Changeable[U],
      thisMemoId: Int): Changeable[U] = {
    val changeable = func()
    datastoreRef ! PutMessage("memo", thisMemoId :: args, changeable)
    changeable
  }*/

  def map[T](arr: Array[Mod[T]], func: (T) => (T)): Array[Mod[T]] = {
    arr.map((elem) =>
      mod((dest: Dest[T]) => read(elem, (value: T) => write(dest, func(value)))))
  }

  def map[T](node: Mod[ListNode[T]], func: (T) => (T)): Mod[ListNode[T]] = {
    def innerMap(dest: Dest[ListNode[T]], lst: ListNode[T]): Changeable[ListNode[T]] = {
      if (lst != null) {
        val newValue = mod((dest: Dest[T]) =>
          read(lst.value, (value: T) => write(dest, func(value))))
        val newNext = mod((dest: Dest[ListNode[T]]) =>
          read(lst.next, (next: ListNode[T]) => innerMap(dest, next)))
        write(dest, new ListNode[T](newValue, newNext))
      } else {
        write(dest, null)
      }
    }
    mod((dest) => read(node, (lst: ListNode[T]) => innerMap(dest, lst)))
  }

  def parMap[T](node: Mod[ListNode[T]], func: (T) => (T)): Mod[ListNode[T]] = {
    def innerMap(tbd: TBD, dest: Dest[ListNode[T]], lst: ListNode[T]):
        Changeable[ListNode[T]] = {
      if (lst != null) {
	      val modTuple =
          tbd.par((tbd: TBD) => {
	          tbd.mod((valueDest: Dest[T]) => {
              tbd.read(lst.value, (value: T) => {
                tbd.write(valueDest, func(value))
              })
            })
	        }, (tbd: TBD) => {
            tbd.mod((nextDest: Dest[ListNode[T]]) => {
	            tbd.read(lst.next, (next: ListNode[T]) => {
                innerMap(tbd, nextDest, next)
              })
            })
	        })
        tbd.write(dest, new ListNode[T](modTuple._1, modTuple._2))
      } else {
        tbd.write(dest, null)
      }
    }
    mod((dest) => read(node, (lst: ListNode[T]) => innerMap(this, dest, lst)))
  }
}
