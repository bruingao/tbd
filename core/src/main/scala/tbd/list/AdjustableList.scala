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

import scala.collection.mutable.Buffer

import tbd.{Context, Mod, Mutator}

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
   * Returns an AdjustableList containing the results of applying the given
   * function to each chunk of this AdjustableList. Only defined for chunked
   * lists.
   */
  def chunkMap[V, W](f: (Vector[(T, U)]) => (V, W))
      (implicit c: Context): AdjustableList[V, W] = ???

  /**
   * Returns a AdjustableList containing all of the elements from this
   * AdjustableList that satisfy the given predicate.
   */
  def filter(pred: ((T, U)) => Boolean)
      (implicit c: Context): AdjustableList[T, U]

  /**
   * Returns an AdjustableList containing each element produced by applying
   * the mapping function to each element of this list.
   */
  def flatMap[V, W](f: ((T, U)) => Iterable[(V, W)])
      (implicit c: Context): AdjustableList[V, W]

  /**
   * Returns an AdjustableList mapping each key that is present in both lists
   * to a pair containing the corresponding values from each list.
   *
   * Generally only defined where the type of that matches this.
   */
  def join[V](that: AdjustableList[T, V], condition: ((T, V), (T, U)) => Boolean)
      (implicit c: Context): AdjustableList[T, (U, V)]

  /**
   * Returns a AdjustableList containing the results of applying the given
   * function to each of the elements of this AdjustableList.
   */
  def map[V, W](f: ((T, U)) => (V, W))
      (implicit c: Context): AdjustableList[V, W]

  /**
   * Returns a AdjustableList containing the results of applying the given
   * function to each of the values of this AdjustableList.
   *
   * Unlike map, if this is sorted, the output will be too.
   */
  def mapValues[V](f: U => V)
      (implicit c: Context): AdjustableList[T, V] = ???

  /**
   * Sorts the list, using the mergesort algorithm.
   */
  def mergesort(comparator: ((T, U), (T, U)) => Int)
      (implicit c: Context): AdjustableList[T, U] = ???

  /**
   * Sorts the list, using the quicksort algorithm.
   */
  def quicksort(comparator: ((T, U), (T, U)) => Int)
      (implicit c: Context): AdjustableList[T, U] = ???

  /**
   * Reduces all elements in the list using f, in an unspecified order.
   */
  def reduce(f: ((T, U), (T, U)) => (T, U))
      (implicit c: Context): Mod[(T, U)]

  /**
   * Reduces all elements with the same key using f.
   */
  def reduceByKey(f: (U, U) => U, comparator: ((T, U), (T, U)) => Int)
      (implicit c: Context): AdjustableList[T, U] = ???

  /**
   * Performs a join by sorting the input lists and then merging them.
   */
  def sortJoin[V](that: AdjustableList[T, V])
      (implicit c: Context, ordering: Ordering[T]): AdjustableList[T, (U, V)]

  /**
   * Returns a tuple of two AdjustableList, whereas the first AdjustableList
   * containins all of the elements from this AdjustableList that satisfy the
   * given predicate, and the second AdjustableList contains all other elements.
   */
  def split(pred: ((T, U)) => Boolean)
      (implicit c: Context): (AdjustableList[T, U], AdjustableList[T, U])

  /* Meta functions */
  def toBuffer(mutator: Mutator): Buffer[(T, U)]
}
