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

import akka.actor.{Actor, ActorSystem, Props}
import akka.pattern.ask
import scala.collection.{GenIterable, GenMap, Seq}
import scala.collection.mutable.{Buffer, Map}
import scala.concurrent.{Await, Future}

import tbd._
import tbd.Constants._
import tbd.datastore.StringData
import tbd.list._

object MapAlgorithm {
  def mapper(pair: (Int, String)): (Int, Int) = {
    var count = 0
    for (word <- pair._2.split("\\W+")) {
      count += 1
    }
    (pair._1, count)
  }
}

class MapActor(partitions: Int) extends Actor {
  import context.dispatcher

  var naiveTable = Buffer[GenIterable[String]]()

  def receive = {
    case table: Map[Int, String] =>
      var remaining = table.values
      for (i <- 1 to partitions) {
	naiveTable += remaining.take(table.size / partitions)
	remaining = remaining.takeRight(table.size / partitions)
      }

    case s: String =>
      val futures = Buffer[Future[GenIterable[Int]]]()
      for (partition <- naiveTable) {
	futures += Future {
	  partition.map(MapAlgorithm.mapper(0, _)._2)
	}
      }
      sender ! Await.result(Future.sequence(futures), DURATION)
  }
}

class MapAlgorithm(_conf: Map[String, _], _listConf: ListConf)
    extends Algorithm[String, AdjustableList[Int, Int]](_conf, _listConf) {
  import scala.concurrent.ExecutionContext.Implicits.global

  val system = ActorSystem("mapSystem")

  val mapActor = system.actorOf(Props(classOf[MapActor], partitions), "datastore")

  val input = ListInput[Int, String](mutator, listConf)

  val data = new StringData(input, count, mutations, Experiment.check)

  //var naiveTable: GenIterable[String] = _
  def generateNaive() {
    data.generate()
    mapActor ! data.table
  }

  def runNaive() {
    Await.result(mapActor ? "run", DURATION)
    system.shutdown()
  }

  private def naiveHelper
      (input: Buffer[GenIterable[String]]): Buffer[GenIterable[Int]] = {
    val futures = Buffer[Future[GenIterable[Int]]]()
    for (partition <- input) {
      futures += Future {
	partition.map(MapAlgorithm.mapper(0, _)._2)
      }
    }
    Await.result(Future.sequence(futures), DURATION)
  }

  def checkOutput(table: Map[Int, String], output: AdjustableList[Int, Int]) = {
    val sortedOutput = output.toBuffer(mutator).map(_._2).sortWith(_ < _)
    val answer = naiveHelper(Buffer(table.values))
    val sortedAnswer = answer.reduce(_ ++ _).toBuffer.sortWith(_ < _)

    //println(sortedOutput)
    //println(sortedAnswer)
    sortedOutput == sortedAnswer
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
