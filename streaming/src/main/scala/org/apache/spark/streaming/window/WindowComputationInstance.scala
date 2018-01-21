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

class WindowComputationInstance[T: ClassTag](
  windowComputation: WindowComputation[T]) {
  def compute(): Iterator[T] = {
    windowComputation.compute()
  }
}

object WindowComputationInstance {
  def newComputationInstance[IN: ClassTag](finalComputation: WindowComputation[_], sourceData: Iterator[IN]):
    WindowComputationInstance[_] = {
    val newComputation = finalComputation.deepCopy()

    def findSource(computation: WindowComputation[_]): WindowComputation[_] = {
      if(computation.prev == null){
        computation
      }else{
        findSource(computation.prev)
      }
    }

    val sourceComputation = findSource(finalComputation)
    if(sourceComputation.isInstanceOf[SourceWindowComputation[IN]]){
      sourceComputation.asInstanceOf[SourceWindowComputation[IN]].sourceData = sourceData
    }

    new WindowComputationInstance(finalComputation)
  }
}
