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
import scala.collection.mutable.Map

import tbd._
import tbd.datastore.GraphData
import tbd.list._

object PageRankAlgorithm {
  val iters = 2
}

class PageRankAdjust(links: AdjustableList[Int, Array[Int]])
  extends Adjustable[AdjustableList[Int, Double]] {

  def run(implicit c: Context) = {
    var ranks = links.mapValues(value => 1.0)

    for (i <- 1 to PageRankAlgorithm.iters) {
      val contribs = links.sortJoin(ranks).flatMap { case (page, (links, rank)) =>
        val size = links.size
        links.map(url => (url, rank / size))
      }

      val reduced = contribs.reduceByKey(_ + _, (pair1: (Int, Double), pair2:(Int, Double)) => pair1._1 - pair2._1)

      ranks = reduced.mapValues(.15 + .85 * _)
    }

    ranks
  }
}


class PageRankAlgorithm(_conf: Map[String, _], _listConf: ListConf)
    extends Algorithm[Array[Int], AdjustableList[Int, Double]](_conf, _listConf) {
  val input = mutator.createList[Int, Array[Int]](listConf)

  val data = new GraphData(input, count, mutations)

  val adjust = new PageRankAdjust(input.getAdjustableList())

  var naiveTable: Map[Int, Array[Int]] = _
  def generateNaive() {
    data.generate()
    naiveTable = data.table
  }

  def runNaive() {
    naiveHelper(naiveTable)
  }

  private def naiveHelper(links: Map[Int, Array[Int]]) = {
    var ranks = links.map(pair => (pair._1, 1.0))

    for (i <- 1 to PageRankAlgorithm.iters) {
      val joined = Map[Int, (Array[Int], Double)]()
      for ((url, rank) <- ranks) {
	joined(url) = (links(url), rank)
      }

      val contribs = joined.values.flatMap { case (links, rank) =>
        val size = links.size
        links.map(url => (url, rank / size))
      }

      val reducedContribs = Map[Int, Double]()
      for ((url, contrib) <- contribs) {
	reducedContribs(url) = contrib + reducedContribs.getOrElse(url, 0.0)
      }
      ranks = reducedContribs.map(pair => (pair._1, .15 + .85 * pair._2))
    }

    ranks
  }

  val epsilon = 0.000001
  def checkOutput(table: Map[Int, Array[Int]], output: AdjustableList[Int, Double]) = {
    val out = output.toBuffer(mutator)
    val answer = naiveHelper(table)

    var check = out.size == answer.size
    for ((node, rank) <- out) {
      if (!answer.contains(node) || (answer(node) - rank).abs > epsilon) {
	check = false
      }
    }

    check
  }
}
