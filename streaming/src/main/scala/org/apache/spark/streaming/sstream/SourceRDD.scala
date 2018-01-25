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

import org.apache.spark.{InterruptibleIterator, Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.{NextIterator, ThreadUtils}

import scala.reflect.ClassTag

private[spark] class SourceFunctionPartition[T: ClassTag](
    var rddId: Long,
    var sourceIndex: Int,
    var sourceFunction: SourceFunction[T]) extends Partition{

  @transient
  private[this] var _executor: ExecutorService = null.asInstanceOf[ExecutorService]

  private[this] def executor(): ExecutorService = synchronized {
    if(_executor == null) {
      _executor = ThreadUtils.newDaemonFixedThreadPool(1,
        s"SourceFunctionPartition_${rddId}_${sourceIndex}_${sourceFunction.toString}")
    }

    _executor
  }

  @transient
  lazy private[this] val defaultOutputCollector: DefaultOutputCollector[T] = new DefaultOutputCollector[T]

  def iterator: Iterator[T] = {
    executor().submit(new Runnable {
      override def run() = {
        sourceFunction.run(defaultOutputCollector)
      }
    })
    defaultOutputCollector.iterator()
  }

  override def hashCode(): Int = {
    (31 * (31 + rddId) + sourceIndex).toInt
  }


  override def equals(other: Any): Boolean = other match {
    case that: SourceFunctionPartition[_] =>
      this.rddId == that.rddId && this.sourceIndex == that.sourceIndex
    case _ => false
  }

  override def index: Int = sourceIndex

}

private class DefaultOutputCollector[T: ClassTag] extends OutputCollector[T] {
  @volatile
  var closed = false

  lazy private[this] val blockingQueue = new LinkedBlockingQueue[T]()

  override def collect(data: T): Unit = {
    blockingQueue.put(data)
  }

  override def close(): Unit = {
    closed = true
  }

  def iterator(): NextIterator[T] = {
    new NextIterator[T] {
    override protected def close() = ???

    override protected def getNext() = {
      if(closed && blockingQueue.isEmpty) {
        finished = true
        null.asInstanceOf[T]
      } else {
        var data = blockingQueue.poll()
        while(data == null){
          data = blockingQueue.poll(100, TimeUnit.MILLISECONDS)
        }
        data
      }
    }
    }
  }
}

private[spark] class SourceRDD[T: ClassTag](
    sc: SparkContext,
    private val sourceFunctions: Array[SourceFunction[T]])
    extends RDD[T](sc, Nil){

  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    new InterruptibleIterator(context, split.asInstanceOf[SourceFunctionPartition[T]].iterator)
  }

  /**
    * Implemented by subclasses to return the set of partitions in this RDD. This method will only
    * be called once, so it is safe to implement a time-consuming computation in it.
    *
    * The partitions in this array must satisfy the following property:
    * `rdd.partitions.zipWithIndex.forall { case (partition, index) => partition.index == index }`
    */
  override protected def getPartitions: Array[Partition] = {
    sourceFunctions.indices.map(
      i => new SourceFunctionPartition[T](id, i, sourceFunctions(i))).toArray
  }
}
