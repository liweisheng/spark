/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.shuffle.pipeline

import org.apache.spark.util.collection.SizeTracker

import scala.collection.mutable.ArrayBuffer


private[spark] class SizeTrackingBuffer[T](initialCapacity: Int = 64)
  extends SizeTracker{
  import SizeTrackingBuffer._

  require(initialCapacity < MAX_CAPACITY, s"Can't make capacity bigger than ${MAX_CAPACITY} elements")
  require(initialCapacity > 1, s"Invalid intial capacity: ${initialCapacity}")

  private var buffer = new ArrayBuffer[T](initialCapacity)

  def append(value: T): Unit = {
    buffer += value
    afterUpdate()
  }

  def iterator: Iterator[T] = {
    buffer.iterator
  }
}

private object SizeTrackingBuffer{
  val MAX_CAPACITY = Int.MaxValue / 2
}
