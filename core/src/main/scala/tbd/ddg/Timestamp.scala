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
package tbd.ddg

object Timestamp {
  // A dummy timestamp which all real Timestamps are less than. Only use for
  // comparison since it isn't actually attached to the ordering data structure.
  val MAX_TIMESTAMP = new Timestamp(new Sublist(Int.MaxValue, null), Int.MaxValue, null)

  // A dummy timestamp which all real Timestamps are greater than.
  val MIN_TIMESTAMP = new Timestamp(new Sublist(-1, null), -1, null)
}

class Timestamp(var sublist: Sublist, var time: Double, var next: Timestamp) {
  var previous: Timestamp = null

  def <(that: Timestamp): Boolean = {
    if (sublist == that.sublist) {
      time < that.time
    } else {
      sublist.id < that.sublist.id
    }
  }

  def >(that: Timestamp): Boolean = {
    if (sublist == that.sublist) {
      time > that.time
    } else {
      sublist.id > that.sublist.id
    }
  }

  override def equals(obj: Any): Boolean = {
    if (!obj.isInstanceOf[Timestamp]) {
      false
    } else {
      val that = obj.asInstanceOf[Timestamp]
      that.time == time && that.sublist.id == sublist.id
    }
  }

  override def toString = "(" + sublist.id + " " + time.toString + ")"
}

class TimestampOrdering extends scala.math.Ordering[Node] {
  def compare(one: Node, two: Node) = {
    if (one.timestamp < two.timestamp) {
      1
    } else if (one.timestamp == two.timestamp) {
      0
    } else {
      -1
    }
  }
}
