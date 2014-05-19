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
package tbd.mod

import akka.actor.ActorRef
import scala.collection.mutable.{ArrayBuffer, Buffer, Set}

import tbd.{Changeable, TBD}
import tbd.datastore.Datastore

class PartitionedChunkList[T, U](
    aPartitions: ArrayBuffer[ChunkList[T, U]]) extends AdjustableList[T, U] {
  val partitions = aPartitions

  def map[V, Q](
      tbd: TBD,
      f: (TBD, T, U) => (V, Q),
      parallel: Boolean = true,
      memoized: Boolean = false): PartitionedChunkList[V, Q] = {
    if (parallel) {
      def innerMemoParMap(tbd: TBD, i: Int): ArrayBuffer[ChunkList[V, Q]] = {
        if (i < partitions.size) {
          val parTup = tbd.par((tbd: TBD) => {
            partitions(i).map(tbd, f, memoized = memoized)
          }, (tbd: TBD) => {
            innerMemoParMap(tbd, i + 1)
          })

          parTup._2 += parTup._1
        } else {
          ArrayBuffer[ChunkList[V, Q]]()
        }
      }

      new PartitionedChunkList(innerMemoParMap(tbd, 0))
    } else {
      new PartitionedChunkList(
        partitions.map((partition: ChunkList[T, U]) => {
          partition.map(tbd, f, memoized = memoized)
        })
      )
    }
  }
  
  def reduce(
      tbd: TBD, 
      initialValueMod: Mod[(T, U)], 
      f: (TBD, T, U, T, U) => (T, U), 
      parallel: Boolean = true,
      memoized: Boolean = true) : Mod[(T, U)] = {
    
    
    def parReduce(tbd: TBD, i: Int): Mod[(T, U)] = {
      if (i < partitions.size) {
        val parTup = tbd.par((tbd: TBD) => {
          partitions(i).reduce(tbd, initialValueMod, f, parallel, memoized)
        }, (tbd: TBD) => {
          parReduce(tbd, i + 1)
        })
       
        
        tbd.mod((dest: Dest[(T, U)]) => {  
          tbd.read2(parTup._1, parTup._2)((a, b) => {
            tbd.write(dest, f(tbd, a._1, a._2, b._1, b._2))     
          })
        })
      } else {
        initialValueMod
      }
    }

    if(parallel) {
      parReduce(tbd, 0)
    } else {
      partitions.map((partition: ChunkList[T, U]) => {
        partition.reduce(tbd, initialValueMod, f, parallel, memoized)
      }).reduce((a, b) => {
        tbd.mod((dest: Dest[(T, U)]) => {  
          tbd.read2(a, b)((a, b) => {
            tbd.write(dest, f(tbd, a._1, a._2, b._1, b._2))     
          })
        })
      })
    }
  }

  def filter(
      tbd: TBD,
      pred: (T, U) => Boolean,
      parallel: Boolean = true,
      memoized: Boolean = true): PartitionedChunkList[T, U] = {
    def parFilter(tbd: TBD, i: Int): ArrayBuffer[ChunkList[T, U]] = {
      if (i < partitions.size) {
        val parTup = tbd.par((tbd: TBD) => {
          partitions(i).filter(tbd, pred, parallel, memoized)
        }, (tbd: TBD) => {
          parFilter(tbd, i + 1)
        })

        parTup._2 += parTup._1
      } else {
        ArrayBuffer[ChunkList[T, U]]()
      }
    }

    if (parallel) {
      new PartitionedChunkList(parFilter(tbd, 0))
    } else {
      new PartitionedChunkList(
        partitions.map((partition: ChunkList[T, U]) => {
          partition.filter(tbd, pred, parallel, memoized)
        })
      )
    }
  }

  /* Meta Operations */
  def toBuffer(): Buffer[U] = {
    val buf = ArrayBuffer[U]()

    for (partition <- partitions) {
      var innerNode = partition.head.read()
      while (innerNode != null) {
        buf ++= innerNode.chunkMod.read().map(pair => pair._2)
        innerNode = innerNode.nextMod.read()
      }
    }

    buf
  }
}
