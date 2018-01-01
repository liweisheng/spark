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
import java.nio.ByteBuffer

import org.apache.spark.internal.Logging
import org.apache.spark.network.buffer.{ManagedBuffer, NettyManagedBuffer}
import org.apache.spark.network.server.SendCallBack
import org.apache.spark.serializer.Serializer
import org.apache.spark.util.io.ChunkedByteBufferOutputStream

import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global


class InMemoryPipelineReaderView[K, V](
  override val pipelineReaderViewId: PipelineReaderViewId,
  override val subPipeline: SubPipeline[K, V],
  override val reduceId: Int,
  override val startSyncId: Long,
  private val serializer: Serializer)
  extends AbstractPipelineReaderView[K, V](
    pipelineReaderViewId, subPipeline, reduceId, startSyncId)
  with Logging{

  val DEFAULT_CHUNK_SIZE = 1024 * 1024
  val ON_HEAP_ALLOCATOR = ByteBuffer.allocate _

  private[this] val waitingFetchIds: mutable.PriorityQueue[Long] = mutable.PriorityQueue.newBuilder
  require(subPipeline.isInstanceOf[InMemorySubPipeline[K, V]],
    "InMemoryPipelineReaderView should instantiate with InMemorySubPipeline")

  val inMemorySubPipeline = subPipeline.asInstanceOf[InMemorySubPipeline[K, V]]

  /**
   * @param fetchId should be auto-incremental
   * @param sendCallback a callback function use to send data
   *
   * */
  override def fetchSegment(fetchId: Long, sendCallback: SendCallBack): Unit = this.synchronized {
    var prepareToSend: ManagedBuffer = null
    if(fetchId == lastFetchId){
      prepareToSend = lastFetchedSegment
      Future {
        sendCallback.send(prepareToSend)
      }
    } else if(fetchId == lastFetchId + 1){
      val dataAndSize = inMemorySubPipeline.fetchPartitionData(reduceId)
      Future {
        serializeAndSend(dataAndSize, sendCallback)
      }
    } else if(fetchId < lastFetchId){
      logWarning(s"fetchId:${fetchId} is less than lastFetchId:${lastFetchId}, omit this fetch." +
        s" reduceId:${reduceId}, pipelineReaderViewId:${pipelineReaderViewId}")
    } else if(fetchId > lastFetchId + 1){
      waitingFetchIds += fetchId
    }
  }

  private[this] def serializeAndSend(dataAndSize: (PipelineDataBlock, Long), sendCallback: SendCallBack): Unit = {
    var chunkSize = Math.min(DEFAULT_CHUNK_SIZE, dataAndSize._2.toInt)
    if(chunkSize == 0){
      chunkSize = DEFAULT_CHUNK_SIZE
    }
    val dataBuffer = dataAndSize._1.asInstanceOf[InMemoryDataBlock[Product2[K, V]]].buffer
    val pipelineEventSerDe = new PipelineEventSerDe[K, V](serializer.newInstance())
    val chunkedByteBufferOutputStream = new ChunkedByteBufferOutputStream(chunkSize, ON_HEAP_ALLOCATOR)
    pipelineEventSerDe.serialize(dataBuffer.iterator, chunkedByteBufferOutputStream)
    val nettyBuf = chunkedByteBufferOutputStream.toChunkedByteBuffer.toNetty

    sendCallback.send(new NettyManagedBuffer(nettyBuf))
  }
}
