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

import org.apache.spark.{OutputSelector, ShuffleDependency, SplitDependency, TaskContext}
import org.apache.spark.shuffle.sort.PipelineShuffleHandle
import org.apache.spark.storage.{BlockManager, PipelineManagerId}

import scala.collection.mutable

private[spark] class PipelineOutputManager[K,V](
  maxPipelineSize: Long,
  pipelineId: PipelineManagerId,
  handle: PipelineShuffleHandle[K,V],
  context: TaskContext,
  blockManager: BlockManager) {
  require(maxPipelineSize > 10 * 1024, s"spark.pipeline.buffer.max should larger than 10M, your value:${maxPipelineSize}")
  private var dep: Either[SplitDependency[K,V], ShuffleDependency[K,V,V]] = _

  private var outputSelector: OutputSelector[K, _] = _
  private var splits: Array[Int] = Array.empty
  private var splitAliases: Array[AnyRef] = Array.empty

  //subpipeline unqiue identifier to subpipeline
  private var subPipelineMap: mutable.Map[Int,InMemorySubPipeline[K, V]] = mutable.HashMap.empty[Int, InMemorySubPipeline[K, V]]

  private var subPipelineSize: mutable.Map[Int, Long] = mutable.HashMap.empty
  //alias to subpipeline unqiue identifier
  private var splitAliasesMap: mutable.Map[AnyRef,Int] =  mutable.HashMap.empty[AnyRef, Int]

  @volatile private[this] var shouldStop: Boolean = false
  @volatile private[this] var stopped: Boolean = false

  private val lock: Object = new Object

  if(handle.isSplitDep){
    dep = Left(handle.dependency.asInstanceOf[SplitDependency[K,V]])
    require(dep.left.get.outputSelector != null, "SplitDependency without OutputSelector specified")
    outputSelector = dep.left.get.outputSelector
    splits = dep.left.get.splits
    splitAliases = dep.left.get.splitsAliases

    val partitioners = dep.left.get.partitioners
    splitAliases.zipWithIndex.foreach{
      case (alias, index) =>
        splitAliasesMap += (alias -> splits(index))
        val subPipeline = new InMemorySubPipeline[K,V](Some(partitioners(index)))
        subPipelineMap += (splits(index) -> subPipeline)
    }

  }else if(handle.isShuffleDep){
    dep = Right(handle.dependency.asInstanceOf[ShuffleDependency[K,V,V]])
    splits = Array.fill[Int](1)(dep.right.get.shuffleId)
    outputSelector = new ShuffleIdSelector[K](Array.fill[Int](1)(dep.right.get.shuffleId))
    splitAliases = Array.fill[AnyRef](1)(dep.right.get.shuffleId)
    splitAliasesMap += (splitAliases(0) -> splits(0))

    val partitioner = dep.right.get.partitioner
    val subPipeline = new InMemorySubPipeline[K, V](Some(partitioner))
    subPipelineMap += (splits(0) -> subPipeline)
  }

  subPipelineMap.keysIterator.foreach{
    case key => subPipelineSize += (key -> 0)
  }

  private[this] def totalSizeInPipeline: Long = {
    subPipelineSize.valuesIterator.reduce(_ + _)
  }

  def writeAll(records: Iterator[PipelineEvent[Product2[K, V]]]): Unit = {
    var event: PipelineEvent[Product2[K, V]] = null

    while(records.hasNext && !shouldStop){
      event = records.next
      if(!event.isData){
         writeNonDataEvent(event)
      }else{
         writeDataEvent(event)
      }
    }
    stopped = true
    lock.synchronized{
      lock.notifyAll()
    }
  }

  def writeNonDataEvent(pipelineEvent: PipelineEvent[Product2[K, V]]): Unit = {
    subPipelineMap.foreach{
      case (subId, subPipeline) =>
        val size = subPipeline.writeNotifyEvent(pipelineEvent)
        subPipelineSize(subId) = size
    }
  }

  def writeDataEvent(pipelineEvent: PipelineEvent[Product2[K, V]]): Unit = {
    val selectedSubPipelines = outputSelector.select(pipelineEvent.data._1)

    splitAliasesMap.filterKeys(key => selectedSubPipelines.contains(key))
      .foreach {
        case (_, subId) =>
          val size = subPipelineMap(subId).writeDataEvent(pipelineEvent)
          subPipelineSize(subId)
      }
  }

  def stop = {
    shouldStop = true
  }

  def isStopped: Boolean = {
    stopped
  }

  def waitForStop = {
    while(!stopped){
      lock.synchronized{
        lock.wait
      }
    }
  }
}

private[spark] object PipelineOutputManager{
  //in bytes
  val SPARK_PIPELINE_BUFFER_MAX = "spark.pipeline.buffer.max"
  val DEFAULT_BUFFER_MAX = (Runtime.getRuntime.maxMemory() * 0.1).toLong
}

private[this] class ShuffleIdSelector[K](
  val shuffleIds: Array[Int]) extends OutputSelector[K, Int]{
  override def select(key: K): Array[Int] = {
     shuffleIds
  }
}
