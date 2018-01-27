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

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.shuffle.pipeline.PipelineManager.OpenedPipelineId
import org.apache.spark._
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.sort.PipelineShuffleHandle
import org.apache.spark.storage.{BlockManager, PipelineManagerId}

import scala.collection.mutable

private[spark] class PipelineManager[K,V](
  val maxPipelineSize: Long,
  val pipelineId: PipelineManagerId,
  val handle: PipelineShuffleHandle[K,V],
  val context: TaskContext,
  val blockManager: BlockManager,
  val pipelineMode: PipelineMode,
  val _serializer: Option[Serializer]) {
  require(maxPipelineSize > 10 * 1024, s"spark.pipeline.buffer.max should larger than 10M, your value:${maxPipelineSize}")
  private var dep: Either[SplitDependency[K,V], ShuffleDependency[K,V,V]] = _

  private val serializer = _serializer.getOrElse(SparkEnv.get.serializer)
  private val subPipelineViewId: AtomicLong = new AtomicLong(0)
  private var outputSelector: OutputSelector[K, _] = _
  private var splits: Array[Int] = Array.empty
  private var splitAliases: Array[Any] = Array.empty

  //subpipeline unqiue identifier to subpipeline
  private var subPipelineMap: mutable.Map[Int, SubPipeline[K, V]] = mutable.HashMap.empty[Int, SubPipeline[K, V]]

  private var subPipelineSize: mutable.Map[Int, Long] = mutable.HashMap.empty

  //alias to subpipeline unqiue identifier
  private var splitAliasesMap: mutable.Map[Any, Int] =  mutable.HashMap.empty[Any, Int]

  // openedPipelineId to uniqueId
  private[this] val openedPipelineReadViewMap: mutable.Map[OpenedPipelineId, Long] = mutable.HashMap.empty
  // uniqueId to pipelineReadView
  private[this] val unqiueId2PipelineReadView = new ConcurrentHashMap[Long, AbstractPipelineReaderView[K, V]]()

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
        val subPipeline = pipelineMode match {
          case _: MemoryPipelineMode =>
            new InMemorySubPipeline[K,V](Some(partitioners(index)))
          case _: SpillablePipelineMode =>
            throw new UnsupportedOperationException
        }

        subPipelineMap += (splits(index) -> subPipeline)
    }

  } else if(handle.isShuffleDep){
    dep = Right(handle.dependency.asInstanceOf[ShuffleDependency[K,V,V]])
    splits = Array.fill[Int](1)(dep.right.get.shuffleId)
    outputSelector = new ShuffleIdSelector[K](Array.fill[Int](1)(dep.right.get.shuffleId))
    splitAliases = Array.fill[Any](1)(dep.right.get.shuffleId)
    splitAliasesMap += (splitAliases(0) -> splits(0))

    val partitioner = dep.right.get.partitioner

    val subPipeline = pipelineMode match {
      case _: MemoryPipelineMode =>
        new InMemorySubPipeline[K,V](Some(partitioner))
      case _: SpillablePipelineMode =>
        //TODO: to support
        throw new UnsupportedOperationException
    }

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

  /**
    * open subpipeline and prepare to send data to downstream reducer.
    *
    * @param subPipelineIndex to locate which subpipeline managed by this manager should be open.
    * @param reduceId
    * @param startSyncId
    * @return (uniqueReadViewId, startSyncId)
    * */
  def openSubPipeline(subPipelineIndex: Int, reduceId: Int, startSyncId: Long): (Long, Long) = this.synchronized{
    val openedPipelineId = (subPipelineIndex, reduceId)

    openedPipelineReadViewMap.get(openedPipelineId) match {
      case Some(uniqueId) =>
        (uniqueId, unqiueId2PipelineReadView.get(uniqueId).lastFetchId)
      case None =>
        val (subPipelineReaderView, id) = createSubPipelineView(subPipelineIndex, reduceId, startSyncId)
        val uniqueId = subPipelineReaderView.pipelineReaderViewId.uniqueId
        openedPipelineReadViewMap(openedPipelineId) = uniqueId
        unqiueId2PipelineReadView.put(uniqueId, subPipelineReaderView)
        (uniqueId, startSyncId)
    }
  }

  def getSubPipelineReaderView(uniqueId: Long): AbstractPipelineReaderView[K, V] = {
    unqiueId2PipelineReadView.get(uniqueId)
  }

  private[this] def createSubPipelineView(subPipelineIndex: Integer, reduceId: Integer, startSyncId: Long): (AbstractPipelineReaderView[K, V], Long) = {
    val subPipeline = subPipelineMap(subPipelineIndex)
    val subPipelineReaderView = subPipeline match {
      case _: InMemorySubPipeline[K, V] =>
        (new InMemoryPipelineReaderView[K, V](
          new PipelineReaderViewId(subPipelineViewId.incrementAndGet()),
          subPipeline,
          reduceId,
          startSyncId,
          serializer), startSyncId)
      case _ =>
        throw new UnsupportedOperationException
    }

    subPipelineReaderView
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

private[spark] object PipelineManager{
  //in bytes
  val SPARK_PIPELINE_BUFFER_MAX = "spark.pipeline.buffer.max"
  val DEFAULT_BUFFER_MAX = (Runtime.getRuntime.maxMemory() * 0.1).toLong

  //(subpipelineIndex, reduceId, startSyncId)
  type OpenedPipelineId = Tuple2[Int, Int]
}

private[this] class ShuffleIdSelector[K](
  val shuffleIds: Array[Int]) extends OutputSelector[K, Int]{
  override def select(key: K): Array[Int] = {
     shuffleIds
  }
}
