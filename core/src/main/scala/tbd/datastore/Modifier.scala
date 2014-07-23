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
package tbd.datastore

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Future

abstract class Modifier(val datastore: Datastore) {

  def insert(key: Any, value: Any): ArrayBuffer[Future[String]]

  def update(key: Any, value: Any): ArrayBuffer[Future[String]]

  def remove(key: Any): ArrayBuffer[Future[String]]

  def getModifiable(): Any
}
