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

package tbd.visualization

import scala.collection.mutable.ArrayBuffer
import tbd.{Adjustable, Changeable, ListConf, ListInput, Mutator, TBD}
import tbd.mod.{AdjustableList, Dest, Mod}
import collection.mutable.HashMap
import scala.util.Random

class TestBase(conf: ListConf) {
  val mutator = new Mutator()
  val input = mutator.createList[Int, Int](conf)

  val table = new HashMap[Int, Int]
  val rand = new Random()
  var freeList = List[Int]()

  var keyCounter = 0
  var maxValue = 100

  def addValue() {
    val newValue = rand.nextInt(maxValue)
    val newKey =
      if(freeList.size == 0 || rand.nextInt(2) == 1) {
        keyCounter += 1
        keyCounter
      } else {
        var (head::tail) = freeList
        freeList = tail
        head
      }

    addValue(newKey, newValue)
  }

  def addValue(key: Int, value: Int) {

    if(table.contains(key)) {
      println("//Element already exists!")
    } else {
      println("m.put(" + key + ", " + value + ")")

      input.put(key, value)
      table += (key -> value)
    }
  }

  def removeValue() {
    val keys = table.keys.toBuffer

    if(keys.length > 0)
    {
      val toRemove = keys(rand.nextInt(keys.length))

      removeValue(toRemove)
    }
  }

  def removeValue(key: Int) {

      println("m.remove(" + key + ") // Was " + table(key))

      table -= key
      input.remove(key)
      freeList = (freeList :+ key)
  }

  def updateValue() {
    val keys = table.keys.toBuffer

    if(keys.length > 0)
    {
      val toUpdate = keys(rand.nextInt(keys.length))
      val newValue = rand.nextInt(maxValue)

      updateValue(toUpdate, newValue)
    }
  }

  def updateValue(key: Int, value: Int) {

      println("m.update(" + key + ", " + value + ")" +
              "// was (" + key + ", " + table(key) + ") ")

      table(key) = value
      input.update(key, value)
  }

  def randomMutation() {
    rand.nextInt(3) match {
      case 0 => updateValue()
      case 1 => removeValue()
      case 2 => addValue()
    }
  }
}