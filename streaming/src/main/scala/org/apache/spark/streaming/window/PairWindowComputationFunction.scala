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

package org.apache.spark.streaming.window

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag


class PairWindowComputationFunction[K: ClassTag, V: ClassTag](
   self: WindowComputation[(K, V)]) {
  def combineByKey[C: ClassTag](
    initializer: V => C,
    combiner: (C, V) => C): Aggregator[K, V, C] ={
    new Aggregator[K, V, C](initializer, combiner)
  }

  def reduceByKey(func: (V, V) => V): WindowComputation[(K, V)]= {
    //TODO: should clean func
    val aggregator = combineByKey(
      (v: V) => v,
      (c: V, v: V) => func(c, v))

    new MappedWindowComputation[(K, V), (K, V)](
      self,
      (iter: Iterator[(K, V)]) => aggregator.insertAll(iter))
  }

  def groupByKey(): WindowComputation[(K, Iterable[V])] = {
    val initializer = (v: V) => ArrayBuffer(v)
    val combiner = (buffer: ArrayBuffer[V], v: V) => buffer += v
    val aggregator = combineByKey(initializer, combiner)
    new MappedWindowComputation[(K, V), (K, Iterable[V])](
      self,
      (iter: Iterator[(K, V)]) => aggregator.insertAll(iter)
    )
  }

  def aggregateByKey[C: ClassTag](zeroValue: C, seqOp: (C, V) => C): WindowComputation[(K, C)] = {
    //TODO: should clean seqOp
    val inInitializerCreator = (v: V) => {
      seqOp(zeroValue, v)
    }

    val aggregator = combineByKey(inInitializerCreator, seqOp)

    new MappedWindowComputation[(K, V), (K, C)](
      self,
      (iter: Iterator[(K, V)]) => aggregator.insertAll(iter))
  }

  def sortByKey(
    ascending: Boolean = true)
    (implicit ordering: Ordering[K]): WindowComputation[(K, V)] = {
    val sorter = new Sorter[K, V](ascending)
    new MappedWindowComputation[(K, V), (K, V)](
      self,
      (iter: Iterator[(K, V)]) => sorter.insertAll(iter))
  }
}
