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

class MappedWindowComputation[T: ClassTag, U: ClassTag](
   override val prev: WindowComputation[T],
   val func : Iterator[T] => Iterator[U]) extends WindowComputation[U](prev){

  override def compute(): Iterator[U] = {
    func(parent().compute())
  }


  override def parent(): WindowComputation[T] = prev

  override def deepCopy(): WindowComputation[U] = {
    val parent = if(prev == null){
      null
    }else{
      prev.deepCopy()
    }

    new MappedWindowComputation(parent, func)
  }
}
