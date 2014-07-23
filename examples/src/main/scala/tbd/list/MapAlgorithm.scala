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

import scala.collection.{GenIterable, GenMap, Seq}
import scala.collection.mutable.Map

import tbd.{Adjustable, ChunkListInput, Context, ListConf, ListInput}
import tbd.mod.AdjustableList

object MapAlgorithm {
  def mapper(pair: (Int, String)): (Int, Int) = {
    var count = 0
    for (word <- pair._2.split("\\W+")) {
      count += 1
    }
    (pair._1, count)
  }
}

class MapAlgorithm(_conf: Map[String, _], _listConf: ListConf)
    extends Algorithm[String, AdjustableList[Int, Int]](_conf, _listConf) {
  val input = mutator.createList[Int, String](listConf)

  data = new WCData(input, count, mutations)

  def runNaive(list: GenIterable[String]) = {
    list.map(MapAlgorithm.mapper(0, _)._2)
  }

  def checkOutput(table: Map[Int, String], output: AdjustableList[Int, Int]) = {
    val sortedOutput = output.toBuffer().sortWith(_ < _)
    val answer = runNaive(table.values)

    sortedOutput == answer.asInstanceOf[GenIterable[Int]].toBuffer.sortWith(_ < _)
  }

  def mapper(pair: (Int, String)) = {
    mapCount += 1
    MapAlgorithm.mapper(pair)
  }

  def run(implicit c: Context) = {
    val pages = input.getAdjustableList()
    pages.map(mapper)
  }
}

class ChunkMapAlgorithm(_conf: Map[String, _], _listConf: ListConf)
    extends Algorithm[String, AdjustableList[Int, Int]](_conf, _listConf) {
  val input = mutator.createChunkList[Int, String](listConf)

  data = new WCData(input, count, mutations)

  def runNaive(list: GenIterable[String]) = {
    list.map(MapAlgorithm.mapper(0, _)._2)
  }

  def checkOutput(table: Map[Int, String], output: AdjustableList[Int, Int]) = {
    val answer = runNaive(table.values)

    output.toBuffer.reduce(_ + _) == answer.asInstanceOf[GenIterable[Int]].reduce(_ + _)
  }

  def chunkMapper(chunk: Vector[(Int, String)]) = {
    mapCount += 1
    var count = 0

    for (page <- chunk) {
      val (key, value) = MapAlgorithm.mapper(page)
      count += value
    }

    (0, count)
  }

  def run(implicit c: Context) = {
    val pages = input.getChunkList()
    pages.chunkMap(chunkMapper)
  }
}
