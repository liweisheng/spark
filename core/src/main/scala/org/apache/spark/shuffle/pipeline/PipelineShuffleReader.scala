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

import org.apache.spark.internal.Logging
import org.apache.spark._
import org.apache.spark.serializer.SerializerManager
import org.apache.spark.shuffle.ShuffleReader
import org.apache.spark.shuffle.sort.PipelineShuffleHandle
import org.apache.spark.storage.BlockManager

/**
 * @author liweisheng
 */
class PipelineShuffleReader[K, C](
    handle: PipelineShuffleHandle[K, C],
    partition: Int,
    context: TaskContext,
    serializerManager: SerializerManager = SparkEnv.get.serializerManager,
    blockManager: BlockManager = SparkEnv.get.blockManager,
    mapOutputTracker: MapOutputTracker = SparkEnv.get.mapOutputTracker)
  extends ShuffleReader[K, C] with Logging{

  private val dep = handle.dependency.asInstanceOf[SplitDependency[K, C]]

  def readStream(): Iterator[PipelineEvent[Product2[K, C]]] = {
    val wrappedPipelineStreams = new PipelineSegmentFetcherIterator(
      context,
      blockManager.shuffleClient,
      mapOutputTracker.getPipelineOutputByExecutorId(handle.shuffleId, partition),
      dep.splitIndex,
      partition)

    val serializerInstance = if(dep.serializer != null) { dep.serializer.newInstance() }
       else {SparkEnv.get.serializer.newInstance()}

    val pipelineEventSerDe = new PipelineEventSerDe[K, C](serializerInstance)

    val recordIter = wrappedPipelineStreams.flatMap {
      case inputStream =>
        pipelineEventSerDe.deserialize(inputStream)
    }

    val interruptibleIter = new InterruptibleIterator[PipelineEvent[Product2[Any, Any]]](context, recordIter)
      .asInstanceOf[Iterator[PipelineEvent[Product2[K, C]]]]

    interruptibleIter
  }

  /** Read the combined key-values for this reduce task */
  override def read(): Iterator[Product2[K, C]] = ???
}
