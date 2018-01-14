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

import org.apache.spark.executor.{CoarseGrainedExecutorBackend, Executor}
import org.apache.spark.{SparkEnv, TaskContext, TaskState}
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{MapStatus, PipelineStatus}
import org.apache.spark.shuffle.ShuffleWriter
import org.apache.spark.shuffle.sort.PipelineShuffleHandle
import org.apache.spark.storage.PipelineManagerId

private[spark] class PipelineShuffleWriter[K, V](
    handle: PipelineShuffleHandle[K, V],
    mapId: Int,
    context: TaskContext)
  extends ShuffleWriter[K, V] with Logging{
  import PipelineManager._

  private val dep = handle.dependency

  private val blockManager = SparkEnv.get.blockManager

  private val pipelineManagerId = PipelineManagerId(System.currentTimeMillis(), mapId)

  private val conf = SparkEnv.get.conf

  private var stopping = false

  private val maxPipelineBufferSizeInBytes = conf.getOption(SPARK_PIPELINE_BUFFER_MAX).map(_.toLong)
    .getOrElse(DEFAULT_BUFFER_MAX)

  private var pipelineOutputManager = new PipelineManager(maxPipelineBufferSizeInBytes,
    pipelineManagerId, handle, context, blockManager, MemoryPipelineMode())


  def writeAll(records: Iterator[PipelineEvent[Product2[K, V]]]): Unit ={
    notifyPipelineStartup(new PipelineStatus(mapId, pipelineManagerId, blockManager.blockManagerId))
    pipelineOutputManager.writeAll(records)
  }

  /**
    * Notify driver the current task has been launched in pipeline mode
    */
  def notifyPipelineStartup(pipelineStatus : PipelineStatus) = {
    val ser = SparkEnv.get.serializer.newInstance()
    val serBytes = ser.serialize(pipelineStatus)
    CoarseGrainedExecutorBackend.get.statusUpdate(context.taskAttemptId(), TaskState.PIPELINE, serBytes)
  }

  def stop(): Unit = {
    if(stopping){
      return
    }

    stopping = true
    if(pipelineOutputManager != null){
      pipelineOutputManager.stop
      pipelineOutputManager.waitForStop
      pipelineOutputManager = null
    }
  }

  /** Write a sequence of records to this task's output */
  override def write(records: Iterator[Product2[K, V]]): Unit = ???

  /** Close this writer, passing along whether the map completed */
  override def stop(success: Boolean): Option[MapStatus] = ???
}
