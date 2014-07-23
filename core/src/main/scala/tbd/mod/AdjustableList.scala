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
import scala.collection.mutable.{Buffer, Set}

import tbd.Context
import tbd.datastore.Datastore

/**
 * A linked list made up of modifiables. The classes that implement this trait
 * each structure their modifiables in different ways for certain performance
 * characteristics.
 *
 * The functions of this class are various list operations, implemented in an
 * self-adjustabling way, so they can be called by an Adjustable.
 */
trait AdjustableList[T, U] {
  /**
   * Returns a AdjustableList containing all of the elements from this
   * AdjustableList that satisfy the given predicate.
   */
  def filter(
      pred: ((T, U)) => Boolean)
     (implicit c: Context): AdjustableList[T, U]

  /**
   * Returns a AdjustableList containing the results of applying the given
   * function to each of the elements of this AdjustableList.
   */
  def map[V, Q](
      f: ((T, U)) => (V, Q))
     (implicit c: Context): AdjustableList[V, Q]

  /**
   * Reduces all elements in the list using f, in an unspecified order,
   * starting with initialValue
   */
  def reduce(
      initialValueMod: Mod[(T, U)],
      f: ((T, U), (T, U)) => (T, U))
     (implicit c: Context): Mod[(T, U)]

  /**
   * Returns a sorted copy of this list.
   */
  def sort(
      comparator: ((T, U), (T, U)) => Boolean)
     (implicit c: Context): AdjustableList[T, U]

  /**
   * Returns a tuple of two AdjustableList, whereas the first AdjustableList
   * containins all of the elements from this AdjustableList that satisfy the
   * given predicate, and the second AdjustableList contains all other elements.
   */
  def split(
      pred: ((T, U)) => Boolean)
     (implicit c: Context): (AdjustableList[T, U], AdjustableList[T, U])

  /* Meta functions */
  def toBuffer(): Buffer[U]
}
