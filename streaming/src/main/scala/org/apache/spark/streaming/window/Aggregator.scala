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

import scala.reflect.ClassTag


class Aggregator[K: ClassTag, V: ClassTag, C: ClassTag](
   val initializer: V => C,
   val combiner: (C, V) => C,
   val ord: Ordering[K] = null){
  private[this] val internalMap = scala.collection.mutable.HashMap.empty[K, C]

  def insertAll(records: Iterator[(K, V)]): Iterator[(K, C)] = {
    while(records.hasNext) {
      val record = records.next()
      val k = record._1
      val v = record._2

      if(!internalMap.contains(k)) {
        internalMap(k) = initializer(v)
      } else {
        internalMap(k) = combiner(internalMap(k), v)
      }
    }
    internalMap.iterator
  }
}
