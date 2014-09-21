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
package tbd.list

import scala.collection.immutable.TreeMap

import tbd.{Mod, Mutator}

class SortedModListInput[T, U](mutator: Mutator)(implicit ordering: Ordering[T])
  extends ListInput[T, U] {

  private var tailMod = mutator.createMod[ModListNode[T, U]](null)

  var nodes = TreeMap[T, Mod[ModListNode[T, U]]]()

  val modList = new ModList[T, U](tailMod, true)

  def put(key: T, value: U) {
    val nextOption = nodes.find { case (_key, _value) => ordering.lt(key, _key) }

    nextOption match {
      case None =>
	val newTail = mutator.createMod[ModListNode[T, U]](null)
	val newNode = new ModListNode((key, value), newTail)

	mutator.updateMod(tailMod, newNode)

	nodes += ((key, tailMod))
	tailMod = newTail
      case Some(nextPair) =>
	val (nextKey, nextMod) = nextPair

        val nextNode = nextMod.read()
	val newNextMod = mutator.createMod[ModListNode[T, U]](nextNode)

	val newNode = new ModListNode((key, value), newNextMod)
	mutator.updateMod(nextMod, newNode)

	nodes += ((nextKey, newNextMod))
	nodes += ((key, nextMod))
    }
  }

  // Note: doesn't really make sense to allow random insertions into a sorted
  // list, so we just ignore the key.
  def putAfter(key: T, newPair: (T, U)) {
    put(newPair._1, newPair._2)
  }

  def update(key: T, value: U) {
    val nextMod = nodes(key).read().nextMod
    val newNode = new ModListNode((key, value), nextMod)

    mutator.updateMod(nodes(key), newNode)
  }

  def remove(key: T) {
    val node = nodes(key).read()
    val nextNode = node.nextMod.read()

    if (nextNode == null) {
      // We're removing the last element in the last.
      assert(tailMod == node.nextMod)
      tailMod = nodes(key)
    } else {
      nodes += ((nextNode.value._1, nodes(key)))
    }

    mutator.updateMod(nodes(key), nextNode)

    nodes -= key
  }

  def contains(key: T): Boolean = {
    nodes.contains(key)
  }

  def getAdjustableList(): AdjustableList[T, U] = {
    modList
  }
}
