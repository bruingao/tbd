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

import scala.collection.{GenIterable, GenMap}
import scala.collection.mutable.Map

import tbd.{Adjustable, ListConf, Mutator}
import tbd.master.Main

abstract class Algorithm[Input, Output](_conf: Map[String, _],
    _listConf: ListConf) extends Adjustable[Output] {
  val conf = _conf
  val listConf = _listConf

  val count = conf("counts").asInstanceOf[String].toInt
  val cacheSize = conf("cacheSizes").asInstanceOf[String].toInt
  val chunkSize = conf("chunkSizes").asInstanceOf[String].toInt
  val mutations = conf("mutations").asInstanceOf[Array[String]]
  val partition = conf("partitions").asInstanceOf[String].toInt
  //val memoized = conf("memoized") == "true"
  val store = conf("store").asInstanceOf[String]

  val main = new Main(store, cacheSize)
  val mutator = new Mutator(main)

  var output: Output = null.asInstanceOf[Output]

  var mapCount = 0
  var reduceCount = 0

  var data: Data[Input] = null.asInstanceOf[Data[Input]]

  def naive(): (Long, Long) = {
    val beforeLoad = System.currentTimeMillis()
    data.loadNaive()
    val naiveTable = Vector(data.naiveTable.values.toSeq: _*).par
    val loadElapsed = System.currentTimeMillis() - beforeLoad

    val before = System.currentTimeMillis()
    runNaive(naiveTable)
    val elapsed = System.currentTimeMillis() - before

    (elapsed, loadElapsed)
  }

  protected def runNaive(table: GenIterable[Input]): Any

  def initial(): (Long, Long) = {
    val beforeLoad = System.currentTimeMillis()
    data.loadInitial()
    val loadElapsed = System.currentTimeMillis() - beforeLoad

    if (!Experiment.check) {
      data.clearValues()
    }

    val before = System.currentTimeMillis()
    output = mutator.run[Output](this)
    val elapsed = System.currentTimeMillis() - before

    if (Experiment.check) {
      assert(checkOutput(data.table, output))
    }

    (elapsed, loadElapsed)
  }

  protected def checkOutput(table: Map[Int, Input], output: Output): Boolean

  def update(count: Double): (Long, Long) = {
    var i = 0
    val beforeLoad = System.currentTimeMillis()
    while (i < count) {
      i += 1
      data.update()
    }
    val loadElapsed = System.currentTimeMillis() - beforeLoad

    val before = System.currentTimeMillis()
    mutator.propagate()
    val elapsed = System.currentTimeMillis() - before

    if (Experiment.check) {
      assert(checkOutput(data.table, output))
    }

    (elapsed, loadElapsed)
  }

  def shutdown() {
    mutator.shutdown()
    main.shutdown()
  }
}
