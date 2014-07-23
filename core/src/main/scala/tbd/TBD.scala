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
import scala.collection.mutable.ListBuffer
import scala.concurrent.{Await, Future}

import tbd.Constants._
import tbd.master.Main
import tbd.messages._
import tbd.mod.{Dest, Mod}

object TBD {
  def read[T, U <: Changeable[_]](mod: Mod[T])(reader: T => U)(implicit c: Context): U = {
    val readNode = c.worker.ddg.addRead(mod.asInstanceOf[Mod[Any]],
					c.currentParent,
					reader.asInstanceOf[Any => Changeable[Any]])

    val outerReader = c.currentParent
    c.currentParent = readNode

    val value = mod.read(c.worker.self)

    val changeable = reader(value)
    c.currentParent = outerReader

    readNode.endTime = c.worker.ddg.nextTimestamp(readNode)
    readNode.currentDest = c.currentDest
    readNode.currentDest2 = c.currentDest2

    changeable
  }

  def read2[T, V, U](
      a: Mod[T],
      b: Mod[V])
     (reader: (T, V) => (Changeable[U]))
     (implicit c: Context): Changeable[U] = {
    read(a) {
      case a => read(b) { case b => reader(a, b) }
    }
  }

  /* readN - Read n mods. Experimental function.
   *
   * Usage Example:
   *
   *  mod {
   *    val a = createMod("Hello");
   *    val b = createMod(12);
   *    val c = createMod("Bla");
   *
   *    readN(a, b, c) {
   *      case Seq(a:String, b:Int, c:String) => {
   *        println(a + b + c)
   *        write(dest, null)
   *      }
   *    }
   *  }
   */
  def readN[U](
      args: Mod[U]*)
     (reader: (Seq[_]) => (Changeable[U]))
     (implicit c: Context): Changeable[U] = {
    readNHelper(args, ListBuffer(), reader)
  }

  private def readNHelper[U](
      mods: Seq[Mod[_]],
      values: ListBuffer[AnyRef],
      reader: (Seq[_]) => (Changeable[U]))
     (implicit c: Context): Changeable[U] = {
    val tail = mods.tail
    val head = mods.head

    read(head) {
      case value =>
	values += value.asInstanceOf[AnyRef]
	if(tail.isEmpty) {
          reader(values.toSeq)
	} else {
          readNHelper(tail, values, reader)
	}
    }
  }

  def mod[T](initializer: => Changeable[T])(implicit c: Context): Mod[T] = {
    val oldCurrentDest = c.currentDest
    c.currentDest = new Dest[T](c.worker.datastoreRef).asInstanceOf[Dest[Any]]
    initializer
    val mod = c.currentDest.mod
    c.currentDest = oldCurrentDest

    mod.asInstanceOf[Mod[T]]
  }

  def mod2[T, U](
      write: Int)
     (initializer: => Changeable2[T, U])
     (implicit c: Context): (Mod[T], Mod[U]) = {
    val oldCurrentDest = c.currentDest
    c.currentDest =
      if (write == 0 || write == 2)
	new Dest[T](c.worker.datastoreRef).asInstanceOf[Dest[Any]]
      else
	c.currentDest

    val oldCurrentDest2 = c.currentDest2
    c.currentDest2 =
      if (write == 1 || write == 2)
	new Dest[T](c.worker.datastoreRef).asInstanceOf[Dest[Any]]
      else
	c.currentDest2

    initializer

    val mod = c.currentDest.mod
    c.currentDest = oldCurrentDest
    val mod2 = c.currentDest2.mod
    c.currentDest2 = oldCurrentDest2

    (mod.asInstanceOf[Mod[T]], mod2.asInstanceOf[Mod[U]])
  }

  def write[T](value: T)(implicit c: Context): Changeable[T] = {
    import c.worker.context.dispatcher

    val awaiting = c.currentDest.mod.update(value)
    Await.result(Future.sequence(awaiting), DURATION)

    val changeable = new Changeable(c.currentDest.mod)
    if (Main.debug) {
      val writeNode = c.worker.ddg.addWrite(changeable.mod.asInstanceOf[Mod[Any]],
                                            c.currentParent)
      writeNode.endTime = c.worker.ddg.nextTimestamp(writeNode)
    }

    changeable.asInstanceOf[Changeable[T]]
  }

  def write2[T, U](value: T, value2: U)(implicit c: Context): Changeable2[T, U] = {
    import c.worker.context.dispatcher

    val awaiting = c.currentDest.mod.update(value)
    val awaiting2 = c.currentDest2.mod.update(value2)
    Await.result(Future.sequence(awaiting), DURATION)
    Await.result(Future.sequence(awaiting2), DURATION)

    write2Helper(c)
  }

  def writeLeft[T, U](
      value: T,
      mod: Mod[U])
     (implicit c: Context): Changeable2[T, U] = {
    import c.worker.context.dispatcher

    if (mod != c.currentDest2.mod) {
      println("WARNING - mod parameter to writeLeft doesn't match currentDest2")
    }

    val awaiting = c.currentDest.mod.update(value)
    Await.result(Future.sequence(awaiting), DURATION)

    write2Helper(c)
  }

  def writeRight[T, U](
      mod: Mod[T],
      value2: U)
     (implicit c: Context): Changeable2[T, U] = {
    import c.worker.context.dispatcher

    if (mod != c.currentDest.mod) {
      println("WARNING - mod parameter to writeRight doesn't match currentDest")
    }

    val awaiting = c.currentDest2.mod.update(value2)
    Await.result(Future.sequence(awaiting), DURATION)

    write2Helper(c)
  }

  private def write2Helper[T, U](c: Context): Changeable2[T, U] = {
    val changeable = new Changeable2(c.currentDest.mod, c.currentDest2.mod)

    if (Main.debug) {
      val writeNode = c.worker.ddg.addWrite(changeable.mod.asInstanceOf[Mod[Any]],
                                            c.currentParent)
      writeNode.mod2 = c.currentDest2.mod
      writeNode.endTime = c.worker.ddg.nextTimestamp(writeNode)
    }

    changeable.asInstanceOf[Changeable2[T, U]]
  }

  def par[T, U](one: Context => T): Parer[T, U] = {
    new Parer(one)
  }

  def makeMemoizer[T](dummy: Boolean = false)(implicit c: Context): Memoizer[T] = {
    c.nextMemoId += 1

    if(dummy) {
      new DummyMemoizer[T](c, c.nextMemoId)
    } else {
      new Memoizer[T](c, c.nextMemoId)
    }
  }

  def createMod[T](value: T)(implicit c: Context): Mod[T] = {
    mod {
      write(value)
    }
  }
}
