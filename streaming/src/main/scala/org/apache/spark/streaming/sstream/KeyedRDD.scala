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

package org.apache.spark.streaming.sstream

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.pipeline.{PipelineEvent, PipelineShuffleReader}

import scala.reflect.ClassTag

private[spark] class KeyedRDDPartition(val idx: Int) extends Partition{
  override def index: Int = idx
}

private[spark] class KeyedRDD[K: ClassTag, V: ClassTag](
    @transient var prev: RDD[PipelineEvent[_ <: Product2[K, V]]],
    part: Partitioner,
    dep: SplitDependency[K, V])
  extends RDD[PipelineEvent[(K, V)]](prev.context, Nil){

  private var userSpecifiedSerializer: Option[Serializer] = None

  override protected def getDependencies: Seq[Dependency[_]] = {
    List(dep)
  }

  override def compute(split: Partition, context: TaskContext): Iterator[PipelineEvent[(K, V)]] = {
    val dep = getDependencies.head.asInstanceOf[SplitDependency[K, V]]
    SparkEnv.get.shuffleManager.getReader(dep.shuffleHandle, split.index, split.index + 1, context)
      .asInstanceOf[PipelineShuffleReader[K, V]]
      .readStream()
      .asInstanceOf[Iterator[PipelineEvent[(K, V)]]]
  }

  override protected def getPartitions: Array[Partition] = {
    Array.tabulate[Partition](part.numPartitions)(i => new KeyedRDDPartition(i))
  }
}
