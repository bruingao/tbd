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

import tbd.{Changeable, TBD}
import tbd.memo.Lift

// The default value of zero for size works because size is only ever
// accessed by the Modifier, which will set it appropriately.
class ChunkListNode[T, U](
    _chunkMod: Mod[Vector[(T, U)]],
    _nextMod: Mod[ChunkListNode[T, U]],
    _size: Int = 0) {
  val chunkMod = _chunkMod
  val nextMod = _nextMod
  val size = _size

  def map[V, Q](
      tbd: TBD,
      dest: Dest[ChunkListNode[V, Q]],
      f: (TBD, T, U) => (V, Q),
      lift: Lift[Mod[ChunkListNode[V, Q]]])
        : Changeable[ChunkListNode[V, Q]] = {
    val newChunkMod = tbd.mod((dest: Dest[Vector[(V, Q)]]) =>
      tbd.read(chunkMod)(chunk =>
        tbd.write(dest, chunk.map(pair => f(tbd, pair._1, pair._2)))))
    val newNextMod = lift.memo(List(nextMod), () =>
      tbd.mod((dest: Dest[ChunkListNode[V, Q]]) =>
        tbd.read(nextMod)(next =>
          if (next != null)
            next.map(tbd, dest, f, lift)
          else
            tbd.write(dest, null))))

    tbd.write(dest, new ChunkListNode[V, Q](newChunkMod, newNextMod))
  }

  def parMap[V, Q](
      tbd: TBD,
      dest: Dest[ChunkListNode[V, Q]],
      f: (TBD, T, U) => (V, Q)): Changeable[ChunkListNode[V, Q]] = {
    val parTuple = tbd.par(
      (tbd: TBD) =>
        tbd.mod((dest: Dest[Vector[(V, Q)]]) =>
          tbd.read(chunkMod)(chunk =>
            tbd.write(dest, chunk.map(pair => f(tbd, pair._1, pair._2))))),
      (tbd: TBD) =>
        tbd.mod((dest: Dest[ChunkListNode[V, Q]]) =>
          tbd.read(nextMod)(next =>
            if (next != null)
              next.parMap(tbd, dest, f)
            else
              tbd.write(dest, null))))

    tbd.write(dest, new ChunkListNode[V, Q](parTuple._1, parTuple._2))
  }
}
