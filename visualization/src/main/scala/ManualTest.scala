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
import scala.util.matching.Regex
import scala.io.StdIn

class ManualTest[T](algorithm: TestAlgorithm[T]) extends TestBase(algorithm.getListConf()) {

  private var propagate = true
  var showDDGEachStep = true
  var initialSize = 10

  private val putm = "(a) (\\d+) (\\d+)".r
  private val updatem = "(u) (\\d+) (\\d+)".r
  private val remm = "(r) (\\d+)".r
  private val propagatem = "(p)".r

  def run() {

    val visualizer = new TbdVisualizer()
    var mutationCounter = 1

    for(i <- 0 to initialSize)
      addValue()

    algorithm.input = input
    val output = mutator.run[T](algorithm)

    println("// Commands: ")
    println("// a KEY VALUE adds a key and value ")
    println("// u KEY VALUE updates a value ")
    println("// r KEY removes a key and value ")
    println("// p propagates ")
    println("")
    println("// KEY and VALUE have to be integers ")


    while(true) {

      while(!propagate) {
        StdIn.readLine() match {
          case putm(_, key, value) => addValue(key.toInt, value.toInt)
          case updatem(_, key, value) => updateValue(key.toInt, value.toInt)
          case remm(_, key) => removeValue(key.toInt)
          case propagatem(_) => propagate = true
          case _ =>
        }
      }
      propagate = false
      mutator.propagate()

      println("m.propagate() // Mutation Cycle " + mutationCounter)
      println("// Current Buffer: " + table.values.toBuffer)

      mutationCounter += 1

      println("// Output: " + output)

      if(!algorithm.checkOutput(output, table)) {
        println("Check error, execution halted.")

        visualizer.showDDG(mutator.getDDG().root)
        while(true) { StdIn.readLine() }
      }

      if(showDDGEachStep) {
        visualizer.showDDG(mutator.getDDG().root)
      }
    }

    mutator.shutdown()
  }
}