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

abstract class WindowComputation[T: ClassTag](
  val prev: WindowComputation[_])
  extends Serializable{
  def compute(): Iterator[T]

  def parent(): WindowComputation[_] = prev

  def deepCopy(): WindowComputation[T]

  def map[U: ClassTag](func: T => U): WindowComputation[U] = {
    //here should clean closure func
    new MappedWindowComputation[T, U](
      this,
      (iter: Iterator[T]) => iter.map(func))
  }

  def mapIterator[U: ClassTag](func: Iterator[T] => U): WindowComputation[U] = {
    new MappedWindowComputation[T, U](
      this,
      (iter: Iterator[T]) => Iterator(func(iter)))
  }

  def filter(func: T => Boolean): WindowComputation[T] = {
    new MappedWindowComputation[T, T](
      this,
      (iter: Iterator[T]) => iter.filter(func))
  }

  def flatMap[U: ClassTag](func: T => TraversableOnce[U]): WindowComputation[U] = {
    new MappedWindowComputation[T, U](
      this,
      (iter: Iterator[T]) => iter.flatMap(func))
  }

  def foldLeft[B: ClassTag](b: B)(op: (B, T) => B): WindowComputation[B]= {
    new MappedWindowComputation[T, B](
      this,
      (iter: Iterator[T]) => Iterator(iter.foldLeft(b)(op))
    )
  }

  def firstN(n: Int = 1): WindowComputation[T] = {
    new MappedWindowComputation[T, T](
      this,
      (iter: Iterator[T]) => {
        val buf = ArrayBuffer[T]()
        val count = 0
        while(iter.hasNext && count < n) {
          val next = iter.next()
          buf.append(next)
        }
        buf.iterator
      }
    )
  }

  def sortBy[K: ClassTag](
    f: T => K,
    ascending: Boolean = true)
    (implicit ord: Ordering[K]): WindowComputation[T] = {
    map(v => {
      (f(v), v)
    }).sortByKey(ascending)(ord).map(_._2)
  }

  def reduce(func: (T, T) => T): WindowComputation[T] = {
    new MappedWindowComputation[T, T](
      this,
      (iter: Iterator[T]) => Iterator(iter.reduceLeft(func))
    )
  }
}

object WindowComputation{
  implicit def toPairWindowComputation[K: ClassTag, V: ClassTag](windowComputation: WindowComputation[(K, V)])

    : PairWindowComputationFunction[K, V] = {
    new PairWindowComputationFunction(windowComputation)
  }

  def newWindowComputation[T: ClassTag](): WindowComputation[T] = {
    new SourceWindowComputation[T](null)
  }
}
