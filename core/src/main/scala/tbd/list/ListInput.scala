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

import scala.collection.mutable.Map

import tbd.{Input, Mutator}

object ListInput {
  def apply[T, U](mutator: Mutator, conf: ListConf = new ListConf())
      (implicit ordering: Ordering[T]): ListInput[T, U] = {
    if (conf.sorted) {
      assert(conf.chunkSize == 1 && conf.partitions == 1)
      new SortedModListInput(mutator)
    }
    else {
      if (conf.partitions == 1) {
	if (conf.chunkSize > 1) {
	  new ChunkListInput(mutator, conf)
	} else {
	  new ModListInput(mutator)
	}
      } else {
	if (conf.chunkSize > 1) {
	  new PartitionedChunkListInput(mutator, conf)
	} else {
	  new PartitionedModListInput(mutator, conf)
	}
      }
    }
  }
}

trait ListInput[T, U] extends Input[T, U] {
  // Inserts all of the elements from data into this ListInput. Assumes that
  // the list is currently empty.
  def load(data: Map[T, U])

  def putAfter(key: T, newPair: (T, U))

  def getAdjustableList(): AdjustableList[T, U]
}