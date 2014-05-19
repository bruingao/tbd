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

import scala.collection.mutable.Buffer

import tbd.TBD

class ChunkList[T, U](
    _head: Mod[ChunkListNode[T, U]]) extends AdjustableList[T, U] {
  val head = _head

  def map[V, Q](
      tbd: TBD,
      f: (TBD, T, U) => (V, Q),
      parallel: Boolean = false,
      memoized: Boolean = true): ChunkList[V, Q] = {
    if (parallel) {
      new ChunkList(
        tbd.mod((dest: Dest[ChunkListNode[V, Q]]) => {
          tbd.read(head)(node => {
            if (node != null) {
              node.parMap(tbd, dest, f)
            } else {
              tbd.write(dest, null)
            }
          })
        })
      )
    } else {
      val lift = tbd.makeLift[Mod[ChunkListNode[V, Q]]](!memoized)
      new ChunkList(
        tbd.mod((dest: Dest[ChunkListNode[V, Q]]) => {
          tbd.read(head)(node => {
            if (node != null) {
              node.map(tbd, dest, f, lift)
            } else {
              tbd.write(dest, null)
            }
          })
        })
      )
    }
  }

  def filter(
      tbd: TBD,
      pred: (T, U) => Boolean,
      parallel: Boolean = false,
      memoized: Boolean = true): ChunkList[T, U] = ???

  def reduce(
      tbd: TBD, 
      initialValueMod: Mod[(T, U)], 
      f: (TBD, T, U, T, U) => (T, U),
      parallel: Boolean = false,
      memoized: Boolean = true) : Mod[(T, U)] = ???

  /* Meta functions */
  def toBuffer(): Buffer[U] = {
    val buf = Buffer[U]()
    var node = head.read()
    while (node != null) {
      buf ++= node.chunkMod.read().map(value => value._2)
      node = node.nextMod.read()
    }

    buf
  }
}
