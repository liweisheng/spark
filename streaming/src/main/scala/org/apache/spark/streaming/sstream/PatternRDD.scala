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

import java.util.concurrent._
import java.util.function.Consumer

import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.shuffle.pipeline.PipelineEvent
import org.apache.spark.streaming.cep.nfa.{CompletedNFAInstance, NFA, NFACompiler, Pattern}
import org.apache.spark.util.{NextIterator, ThreadUtils}

import scala.reflect.ClassTag


private[spark] class PatternRDD[T: ClassTag, U: ClassTag](
    @transient var prev: RDD[PipelineEvent[T]],
    pattern: Pattern[T],
    patternProcessor: PatternProcessor[T, U])
  extends RDD[PipelineEvent[U]](prev) {

  @transient
  private[this] var _executor: ExecutorService = null.asInstanceOf[ExecutorService]

  private[this] def executor(partitionId: Int): ExecutorService = synchronized {
    if(_executor == null) {
      _executor = ThreadUtils.newDaemonFixedThreadPool(1,
        s"PatternRDD_${id}_${partitionId}_${patternProcessor.toString}")
    }

    _executor
  }

  override def compute(split: Partition, context: TaskContext): Iterator[PipelineEvent[U]] = {
    val parentIter = firstParent[PipelineEvent[T]].iterator(split, context)
    val nfaFactory = NFACompiler.newCompiler[T](pattern).compile()
    val nfa = nfaFactory.createNFA()

    val patternProcessorTask = new PatternProcessorTask[T, U](parentIter, patternProcessor, nfa)
    executor(split.index).submit(patternProcessorTask)
    patternProcessorTask.iterator()
  }

  override protected def getPartitions: Array[Partition] = {
    firstParent[T].partitions
  }
}

class PatternProcessorTask[T: ClassTag, U: ClassTag](
  val parentIter: Iterator[PipelineEvent[T]],
  val patternProcessor: PatternProcessor[T, U],
  val nfa: NFA[T]) extends Runnable {
  private val blockingQueue = new LinkedBlockingQueue[PipelineEvent[U]]()

  @volatile
  private var finished = false

  override def run(): Unit = {
    while(parentIter.hasNext) {
      val next = parentIter.next()
      if(next.isData) {
        val data = next.data
        val eventTime = next.eventTime
        val patterns = nfa.processElement(data, eventTime)
        val matchedPatterns = patterns.p1
        val timeoutPatternPatterns = patterns.p2

        if(!matchedPatterns.isEmpty) {
          matchedPatterns.forEach(
            new Consumer[CompletedNFAInstance[T]] {
              override def accept(t: CompletedNFAInstance[T]): Unit = {
                val ret = patternProcessor.processMatchedPattern(t.getStartTimestamp, t.getPattern2Events)
                if(ret != null) {
                  blockingQueue.put(PipelineEvent.dataEvent(ret, System.currentTimeMillis()))
                }
              }
            }
          )
        }

        if(!timeoutPatternPatterns.isEmpty) {
          matchedPatterns.forEach(
            new Consumer[CompletedNFAInstance[T]] {
              override def accept(t: CompletedNFAInstance[T]): Unit = {
                val ret = patternProcessor.processTimeoutPattern(t.getStartTimestamp, t.getPattern2Events)
                if(ret != null) {
                  blockingQueue.put(PipelineEvent.dataEvent(ret, System.currentTimeMillis()))
                }
              }
            }
          )
        }
      }else {
        //TODO: should process watermark and checkpoint
        blockingQueue.put(PipelineEvent.convertNonDataEvent[U](next))
      }
    }
    finished = true
  }

  def iterator(): NextIterator[PipelineEvent[U]] = {
    new NextIterator[PipelineEvent[U]] {

      override protected def close() = {}

      override protected def getNext() = {
        var data = blockingQueue.poll()
        while(data == null && !finished) {
          finished = PatternProcessorTask.this.finished
          data = blockingQueue.poll(100, TimeUnit.MILLISECONDS)
        }
        data
      }
    }
  }
}

abstract class PatternProcessor[T: ClassTag, U: ClassTag] extends Serializable{
  def processMatchedPattern(startTimestamp: Long, patterns: java.util.Map[String, java.util.List[T]]): U

  def processTimeoutPattern(startTimestamp: Long, patterns: java.util.Map[String, java.util.List[T]]): U
}
