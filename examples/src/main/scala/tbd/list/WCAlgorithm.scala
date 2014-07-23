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
import scala.collection.immutable.HashMap

import tbd.{Adjustable, ChunkListInput, Context, ListConf}
import tbd.mod.Mod
import tbd.TBD._

object WCAlgorithm {
  def wordcount(s: String): HashMap[String, Int] = {
    HashMap(mutableWordcount(s).toSeq: _*)
  }

  def mutableWordcount(s: String, counts: Map[String, Int] = Map[String, Int]())
      : Map[String, Int] = {
    for (word <- s.split("\\W+")) {
      if (counts.contains(word)) {
        counts(word) += 1
      } else {
        counts(word) = 1
      }
    }

    counts
  }

  def countReduce(s: String, counts: Map[String, Int]): Map[String, Int] = {
    for (word <- s.split("\\W+")) {
      if (counts.contains(word)) {
        counts(word) += 1
      } else {
        counts(word) = 1
      }
    }

    counts
  }

  def reduce(map1: HashMap[String, Int], map2: HashMap[String, Int])
      : HashMap[String, Int] = {
    map1.merged(map2)({ case ((k, v1),(_, v2)) => (k, v1 + v2)})
  }

  def mutableReduce(map1: Map[String, Int], map2: Map[String, Int])
      : Map[String, Int] = {
    val counts = map2.clone()
    for ((key, value) <- map1) {
      if (counts.contains(key)) {
        counts(key) += map1(key)
      } else {
        counts(key) = map1(key)
      }
    }
    counts
  }
}

class WCAlgorithm(_conf: Map[String, _], _listConf: ListConf)
    extends Algorithm[String, Mod[(Int, HashMap[String, Int])]](_conf, _listConf) {
  val input = mutator.createList[Int, String](listConf)

  data = new WCData(input, count, mutations)

  def runNaive(list: GenIterable[String]) = {
    list.aggregate(Map[String, Int]())((x, line) =>
      WCAlgorithm.countReduce(line, x), WCAlgorithm.mutableReduce)
  }

  def checkOutput(
      table: Map[Int, String],
      output: Mod[(Int, HashMap[String, Int])]) = {
    val answer = runNaive(table.values)
    output.read()._2 == answer
  }

  def mapper(pair: (Int, String)) = {
    mapCount += 1
    (pair._1, WCAlgorithm.wordcount(pair._2))
  }

  def reducer(
      pair1: (Int, HashMap[String, Int]),
      pair2: (Int, HashMap[String, Int])) = {
    reduceCount += 1
    (pair1._1, WCAlgorithm.reduce(pair1._2, pair2._2))
   }

  def run(implicit c: Context): Mod[(Int, HashMap[String, Int])] = {
    val pages = input.getAdjustableList()
    val counts = pages.map(mapper)
    val initialValue = createMod((0, HashMap[String, Int]()))
    counts.reduce(initialValue, reducer)
  }
}

class ChunkWCAlgorithm(_conf: Map[String, _], _listConf: ListConf)
    extends Algorithm[String, Mod[(Int, HashMap[String, Int])]](_conf, _listConf) {
  val input = mutator.createChunkList[Int, String](listConf)

  data = new WCData(input, count, mutations)

  def runNaive(list: GenIterable[String]) = {
    list.aggregate(Map[String, Int]())((x, line) =>
      WCAlgorithm.countReduce(line, x), WCAlgorithm.mutableReduce)
  }

  def checkOutput(
      table: Map[Int, String],
      output: Mod[(Int, HashMap[String, Int])]) = {
    val answer = runNaive(table.values)
    output.read()._2 == answer
  }

  def chunkMapper(chunk: Vector[(Int, String)]) = {
    mapCount += 1
    var counts = Map[String, Int]()

    for (page <- chunk) {
      counts = WCAlgorithm.mutableWordcount(page._2, counts)
    }

    (0, HashMap(counts.toSeq: _*))
  }

  def chunkReducer(
      pair1: (Int, HashMap[String, Int]),
      pair2: (Int, HashMap[String, Int])) = {
    reduceCount += 1
    (pair1._1, WCAlgorithm.reduce(pair1._2, pair2._2))
  }

  def run(implicit c: Context): Mod[(Int, HashMap[String, Int])] = {
    val pages = input.getChunkList()
    val counts = pages.chunkMap(chunkMapper)
    val initialValue = createMod((0, HashMap[String, Int]()))
    counts.reduce(initialValue, chunkReducer)
  }
}
