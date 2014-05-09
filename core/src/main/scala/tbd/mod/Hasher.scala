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

class Hasher(
  k: Int,
  m: Int) {

  def binaryHash(id: ModId, round: Int) = {
    hash(id.value.hashCode() ^ round) == 0
  }

  var coefs:List[BigInt] = null
  def hash(x: Int) = {
    val p = BigInt(1073741789)


    if(coefs == null) {
      coefs = List()
      val rand = new scala.util.Random()
      for(i <- 0 to k) {
        coefs = BigInt(rand.nextInt(1000) + 1) :: coefs
      }
    }

    val bigX = BigInt(x)
    val bigM = BigInt(m)

    val (s,_) = coefs.foldLeft((BigInt(0), BigInt(k))) {
      (t, c) => (t._1 + c * bigX.modPow(t._2, p), t._2 - 1)
    }

    s.mod(bigM).toInt
  }
}
