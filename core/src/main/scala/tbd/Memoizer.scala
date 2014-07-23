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

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{Await, Future}

import tbd.Constants._
import tbd.ddg.MemoNode
import tbd.master.Master
import tbd.mod.Mod

class Memoizer[T](c: Context, memoId: Int) {
  import c.worker.context.dispatcher

  def apply(args: Any*)(func: => T): T = {
    import c._
    val signature = memoId +: args

    var found = false
    var ret = null.asInstanceOf[T]
    if (!initialRun) {
      if (!updated(args)) {
	if (worker.memoTable.contains(signature)) {

          // Search through the memo entries matching this signature to see if
          // there's one in the right time range.
          for (memoNode <- worker.memoTable(signature)) {
            val timestamp = memoNode.timestamp
            if (!found && timestamp > reexecutionStart &&
		timestamp < reexecutionEnd &&
		memoNode.matchableInEpoch <= Master.epoch) {

	      if (memoNode.currentDest != currentDest &&
		  memoNode.value.isInstanceOf[Changeable[_]]) {
                val changeable = memoNode.value.asInstanceOf[Changeable[Any]]

                val awaiting = currentDest.mod.update(changeable.mod.read())
                Await.result(Future.sequence(awaiting), DURATION)

		worker.ddg.replaceDests(memoNode,
					    memoNode.currentDest,
					    currentDest)
              }

	      if (memoNode.value.isInstanceOf[Changeable2[_, _]] &&
		  memoNode.currentDest2 != currentDest2) {
		val changeable2 = memoNode.value.asInstanceOf[Changeable2[Any, Any]]

		val awaiting = currentDest2.mod.update(changeable2.mod2.read())
		Await.result(Future.sequence(awaiting), DURATION)

		worker.ddg.replaceDests(memoNode,
					    memoNode.currentDest2,
					    currentDest2)
	      }

              found = true
              worker.ddg.attachSubtree(currentParent, memoNode)

	      memoNode.matchableInEpoch = Master.epoch + 1
              ret = memoNode.value.asInstanceOf[T]

	      // This ensures that we won't match anything under the currently
	      // reexecuting read that comes before this memo node, since then
	      // the timestamps would be out of order.
	      reexecutionStart = memoNode.endTime

              val future = worker.propagate(timestamp,
                                                memoNode.endTime)
              Await.result(future, DURATION)
            }
          }
	}
      }
    }

    if (!found) {
      val memoNode = worker.ddg.addMemo(currentParent, signature)
      val outerParent = currentParent
      currentParent = memoNode
      val value = func
      currentParent = outerParent
      memoNode.endTime = worker.ddg.nextTimestamp(memoNode)
      memoNode.currentDest = currentDest
      memoNode.currentDest2 = currentDest2
      memoNode.value = value

      if (worker.memoTable.contains(signature)) {
        worker.memoTable(signature) += memoNode
      } else {
        worker.memoTable += (signature -> ArrayBuffer(memoNode))
      }

      ret = value
    }

    ret
  }

  private def updated(args: Seq[_]): Boolean = {
    var updated = false

    for (arg <- args) {
      if (arg.isInstanceOf[Mod[_]]) {
        if (c.updatedMods.contains(arg.asInstanceOf[Mod[_]].id)) {
	  updated = true
        }
      }
    }

    updated
  }
}

class DummyMemoizer[T](c: Context, memoId: Int) extends Memoizer[T](c, memoId) {
  override def apply(args: Any*)(func: => T): T = {
    func
  }
}
