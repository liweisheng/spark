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

import org.apache.spark.Partitioner

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

private[spark] class InMemorySubPipeline[K,V](
    val partitioner: Option[Partitioner])
  extends SubPipeline[K, V]{

  @volatile
  private[this] var receiveBlockEndEvent = false

  @volatile
  private var isStop: Boolean = false

  private val numPartition = partitioner.map(_.numPartitions).getOrElse(1)
  private val shouldPartition = numPartition > 1

  private def getPartition(key: K): Int = {
    if(shouldPartition) partitioner.get.getPartition(key) else 0
  }

  private val subPartitions : Array[SubPartition[K,V]] = new Array(numPartition)
  private val subPartitionSizes: Array[Long] = Array.fill[Long](numPartition)(0)

  private def totalSize: Long = {
    subPartitionSizes.reduce(_ + _)
  }

  for(i <- 0 until numPartition){
    subPartitions(i) = new SubPartition[K,V](i)
  }

  def writeDataEvent(event : PipelineEvent[Product2[K,V]]): Long = {
    if(event.isBlockEnd){

    }
    val key = event.data._1
    val targetPartition = getPartition(key)
    val size = subPartitions(targetPartition).writeEvent(event)
    subPartitionSizes(targetPartition) = size
    totalSize
  }

  def writeNotifyEvent(event: PipelineEvent[Product2[K,V]]): Long = {
    subPartitions.zipWithIndex.foreach{
      case (subPartition, index) =>
        val size = subPartition.writeEvent(event)
        subPartitionSizes(index) = size
    }

    totalSize
  }

  def hasMoreData(): Boolean = {
    !receiveBlockEndEvent
  }

  private[this] def processBlockEnd() = {
    receiveBlockEndEvent = true
  }

  /**
    * @return data buffer iterator and its estimated size
    */
  def fetchPartitionData(part: Int): (PipelineDataBlock, Long) = {
    val buffer = subPartitions(part).getSnapshot()
    (new InMemoryDataBlock(buffer), buffer.estimateSize())
  }

  override def stop(): Unit = {
    isStop = true
  }

  override def isStopped(): Boolean = {isStop}
}

private[spark] class SubPartition[K,V](
    val part: Int){

  @volatile var buffer = new SizeTrackingBuffer[PipelineEvent[Product2[K,V]]]

  /**
   * @return size of buffer
   */
  def writeEvent(event: PipelineEvent[Product2[K,V]]): Long = {
    buffer.append(event)
    buffer.estimateSize()
  }

  def getSnapshot(): SizeTrackingBuffer[PipelineEvent[Product2[K,V]]] = {
    val oldBuffer = buffer;
    buffer = new SizeTrackingBuffer[PipelineEvent[Product2[K, V]]]
    oldBuffer
  }
}
