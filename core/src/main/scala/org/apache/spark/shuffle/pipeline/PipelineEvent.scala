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

import java.io.{ByteArrayOutputStream, IOException, ObjectOutputStream}
import java.nio.ByteBuffer

import org.apache.spark.util.io.ChunkedByteBufferOutputStream

import scala.reflect.ClassTag

private[spark] class PipelineEvent[DATATYPE](
    val eventType: PipelineEvent.EvenType,
    val data: DATATYPE,
    val nonData: Any,
    val eventTime: Long){
  import PipelineEvent._

  def isData: Boolean = {
    eventType == DATA
  }

  def isBlockEnd: Boolean = {
    eventType == BLOCKEND
  }

  def isCheckpoint: Boolean = {
    eventType == CHECKPOINT
  }

  def isWaterMark: Boolean = {
    eventType == WATERMARK
  }
}

private[spark] object PipelineEvent {
  type EvenType = Byte

  val DATA: EvenType = 0.asInstanceOf[Byte]
  val CHECKPOINT: EvenType = 1.asInstanceOf[Byte]
  val WATERMARK: EvenType = 2.asInstanceOf[Byte]
  val BLOCKEND: EvenType = 3.asInstanceOf[Byte]

  val BLOCK_END_EVENT: PipelineEvent[Any] = new PipelineEvent[Any](BLOCKEND, null, null, Long.MaxValue)

  def dataEvent[T: ClassTag](data: T, eventTime: Long): PipelineEvent[T] = {
    new PipelineEvent[T](
      DATA,
      data,
      null,
      eventTime)
  }

  def convertNonDataEvent[THAT](event: PipelineEvent[_]): PipelineEvent[THAT] = {
    new PipelineEvent(
      event.eventType,
      null.asInstanceOf[THAT],
      event.nonData,
      event.eventTime)
  }

}
