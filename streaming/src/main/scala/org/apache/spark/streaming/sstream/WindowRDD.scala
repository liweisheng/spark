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

import java.util.concurrent.{ExecutorService, Executors, LinkedBlockingDeque, TimeUnit}

import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.shuffle.pipeline.PipelineEvent
import org.apache.spark.streaming.window.{TriggerCallback, Window, WindowIdentifier}
import org.apache.spark.util.NextIterator

import scala.reflect.ClassTag

private[spark] class WindowRDD[IN: ClassTag, OUT: ClassTag](
    var prev: RDD[PipelineEvent[IN]],
    private val window: Window[IN, OUT])
  extends RDD[PipelineEvent[(WindowIdentifier, Iterator[OUT])]](prev){

  require(window != null, "window should not be null!")

  private[this] val defaultTriggerCallback = new DefaultTriggerCallback[OUT]

  @transient
  private[this] var _executor: ExecutorService = null.asInstanceOf[ExecutorService]

  private[this] def executor(): ExecutorService = {
    if(_executor == null) {
      _executor = Executors.newFixedThreadPool(1)
    }

    _executor
  }

  override def compute(split: Partition, context: TaskContext):
    Iterator[PipelineEvent[(WindowIdentifier, Iterator[OUT])]] = {
    window.start(defaultTriggerCallback)
    executor.submit(new Runnable {
      override def run() = {
        val parentIter = firstParent[PipelineEvent[IN]].iterator(split, context)
        while(parentIter.hasNext) {
          val next = parentIter.next()
          if(next.isData) {
            window.processElement(next.data, next.eventTime)
          } else{
            defaultTriggerCallback.put(
              new PipelineEvent(
                next.eventType,
                null.asInstanceOf[(WindowIdentifier, Iterator[OUT])],
                next.nonData,
                next.eventTime))
          }
        }
      }
    })
    defaultTriggerCallback.iterator()
  }

  override protected def getPartitions: Array[Partition] = {
    firstParent[IN].partitions
  }
}

private class DefaultTriggerCallback[OUT: ClassTag] extends TriggerCallback[OUT]{
  private val blockingQueue = new LinkedBlockingDeque[
    PipelineEvent[(WindowIdentifier, Iterator[OUT])]]()

  override def call(windowIdentifier: WindowIdentifier, computedData: Iterator[OUT]): Unit = {
    val pipelineEvent = PipelineEvent.dataEvent((windowIdentifier, computedData), windowIdentifier.startTimestamp)
    put(pipelineEvent)
  }

  def put(event: PipelineEvent[(WindowIdentifier, Iterator[OUT])]) = {
    blockingQueue.put(event)
  }

  def iterator(): NextIterator[PipelineEvent[(WindowIdentifier, Iterator[OUT])]] = {
    new NextIterator[PipelineEvent[(WindowIdentifier, Iterator[OUT])]] {

      override protected def close() = {}

      override protected def getNext() = {
        var data = blockingQueue.poll()
        while(data == null) {
          data = blockingQueue.poll(100, TimeUnit.MILLISECONDS)
        }
        data
      }
    }
  }
}