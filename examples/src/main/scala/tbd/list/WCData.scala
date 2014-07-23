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
package tbd.examples.list

import scala.collection.GenIterable
import scala.collection.mutable.{ArrayBuffer, Map}

import tbd.{Input, Mutator}

class WCData(input: Input[Int, String], count: Int, mutations: Array[String])
    extends Data[String] {
  val maxKey = count * 10

  val rand = new scala.util.Random()

  private def loadPages(
      table: Map[Int, String],
      chunks: ArrayBuffer[String],
      count: Int) {
    while (table.size < count) {
      val elems = scala.xml.XML.loadFile("wiki.xml")

      var i = table.size
      (elems \\ "elem").map(elem => {
        (elem \\ "value").map(value => {
          if (table.size < count) {
            table += (i -> value.text)
            i += 1
          } else {
	    chunks += value.text
          }
        })
      })
    }
  }

  private def loadChunks(
      chunks: ArrayBuffer[String]) {
    val elems = scala.xml.XML.loadFile("wiki.xml")

    var i = 0
    (elems \\ "elem").map(elem => {
      (elem \\ "value").map(value => {
	chunks += value.text
      })
    })
  }

  val naiveChunks = ArrayBuffer[String]()
  def loadNaive() {
    loadPages(naiveTable, naiveChunks, count)
  }

  val chunks = new ArrayBuffer[String]()
  def loadInitial() {
    loadPages(table, chunks, count)

    for (pair <- table) {
      input.put(pair._1, pair._2)
    }
  }

  def clearValues() {
    for ((key, value) <- table) {
      table(key) = ""
    }
  }

  def update() {
    mutations(rand.nextInt(mutations.size)) match {
      case "insert" => addValue()
      case "remove" => removeValue()
      case "update" => updateValue()
    }
  }

  def addValue() {
    if (chunks.size == 0) {
      loadChunks(chunks)
    }

    var key = rand.nextInt(maxKey)
    val value = rand.nextInt(Int.MaxValue)
    while (table.contains(key)) {
      key = rand.nextInt(maxKey)
    }
    input.put(key, chunks.head)

    if (Experiment.check) {
      table += (key -> chunks.head)
    } else {
      table += (key -> "")
    }

    chunks -= chunks.head
  }

  def removeValue() {
    if (table.size > 1) {
      var key = rand.nextInt(maxKey)
      while (!table.contains(key)) {
        key = rand.nextInt(maxKey)
      }
      input.remove(key)
      table -= key
    } else {
      addValue()
    }
  }

  def updateValue() {
    if (chunks.size == 0) {
      loadChunks(chunks)
    }

    var key = rand.nextInt(maxKey)
    val value = rand.nextInt(Int.MaxValue)
    while (!table.contains(key)) {
      key = rand.nextInt(maxKey)
    }
    input.update(key, chunks.head)

    if (Experiment.check) {
      table(key) = chunks.head
    }

    chunks -= chunks.head
  }

  def updateNaive() {
    mutations(rand.nextInt(mutations.size)) match {
      case "insert" => addValueNaive()
      case "remove" => removeValueNaive()
      case "update" => updateValueNaive()
    }
  }

  def addValueNaive() {
    if (naiveChunks.size == 0) {
      loadChunks(naiveChunks)
    }

    var key = rand.nextInt(maxKey)
    val value = rand.nextInt(Int.MaxValue)
    while (naiveTable.contains(key)) {
      key = rand.nextInt(maxKey)
    }

    if (Experiment.check) {
      naiveTable += (key -> naiveChunks.head)
    } else {
      naiveTable += (key -> "")
    }

    naiveChunks -= naiveChunks.head
  }

  def removeValueNaive() {
    if (naiveTable.size > 1) {
      var key = rand.nextInt(maxKey)
      while (!naiveTable.contains(key)) {
        key = rand.nextInt(maxKey)
      }

      naiveTable -= key
    } else {
      addValueNaive()
    }
  }

  def updateValueNaive() {
    if (naiveChunks.size == 0) {
      loadChunks(naiveChunks)
    }

    var key = rand.nextInt(maxKey)
    val value = rand.nextInt(Int.MaxValue)
    while (!naiveTable.contains(key)) {
      key = rand.nextInt(maxKey)
    }

    if (Experiment.check) {
      naiveTable(key) = naiveChunks.head
    } else {
      naiveTable(key) = ""
    }

    naiveChunks -= naiveChunks.head
  }
}
