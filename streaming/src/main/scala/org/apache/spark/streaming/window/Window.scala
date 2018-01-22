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

package org.apache.spark.streaming.window

import java.util.concurrent.{Executors, TimeUnit}

import org.apache.spark.streaming.window.TriggerPolicy.TriggerPolicy

import scala.concurrent.duration.Duration
import scala.reflect.ClassTag


abstract class Window[IN: ClassTag, OUT: ClassTag] extends Serializable{
  private[this] var triggerPolicy: TriggerPolicy = _
  private[this] var computation: WindowComputation[OUT] = _

  private[this] var triggerCallback: TriggerCallback[OUT] = _

  def computeWindow(datasInWindow: Iterable[IN]): Iterator[OUT] = {
    val windowComputationInstance = WindowComputationInstance.newComputationInstance[IN](computation,
      datasInWindow.iterator)
    windowComputationInstance.compute().asInstanceOf[Iterator[OUT]]
  }

  def computation(windowComputation: WindowComputation[OUT]): this.type = {
    computation = windowComputation
    this
  }

  def processElement(e: IN, timestamp: Long)
  def processWatermark[W: ClassTag](watermark: W)
  def processCheckpoint[C: ClassTag](checkpoint: C)

  def start(triggerCallback: TriggerCallback[OUT]) = {
    this.triggerCallback = triggerCallback
  }

  def stop()

  def triggerBy(triggerPolicy: TriggerPolicy): this.type = {
    this.triggerPolicy = triggerPolicy
    this
  }

  protected def onTrigger(windowIdentifier: WindowIdentifier, computedData: Iterator[OUT]) = {
    triggerCallback.call(windowIdentifier, computedData)
  }

  protected def newComputationInstance(datasInWindow: Iterator[IN]): WindowComputationInstance[OUT] = {
    WindowComputationInstance.newComputationInstance(computation, datasInWindow)
      .asInstanceOf[WindowComputationInstance[OUT]]
  }
}

class SlideWindow[IN: ClassTag, OUT: ClassTag](
  private val slideDuration: Duration,
  private val windowDuration: Duration,
  private val allowedLatency: Duration = Duration(0, TimeUnit.MILLISECONDS))
  extends Window[IN, OUT]{

  private[this] val executor = Executors.newScheduledThreadPool(1)

  private[this] val windowBuffer = new TimeWindowBuffer[IN](slideDuration, windowDuration, allowedLatency)

  private[this] val periodicTriggerTask = new Runnable {
    override def run() = {
      val windows = windowBuffer.triggerNext()
      println(s"task invoked at ${System.currentTimeMillis()}")
      for(window <- windows){
        if(!window._2.isEmpty){
          val newWindowComputation = newComputationInstance(window._2.iterator)
          val computationResult = newWindowComputation.compute()
          onTrigger(window._1, computationResult)
        }
      }
    }
  }

  override def processElement(e: IN, timestamp: Long): Unit = {
    val delayedWindows = windowBuffer.insert(e, timestamp)
    for(window <- delayedWindows) {
      if(!window._2.isEmpty){
        val newWindowComputation = newComputationInstance(window._2.iterator)
        val computationResult = newWindowComputation.compute()
        onTrigger(window._1, computationResult)
      }
    }
  }

  override def processWatermark[W: ClassTag](watermark: W): Unit = {

  }

  override def processCheckpoint[C: ClassTag](checkpoint: C): Unit = {

  }

  override def start(triggerCallback: TriggerCallback[OUT]): Unit = {
    super.start(triggerCallback)
    println("start time window...")
    executor.scheduleAtFixedRate(
      periodicTriggerTask,
      allowedLatency.toMillis,
      slideDuration.toMillis,
      TimeUnit.MILLISECONDS)
  }

  override def stop(): Unit = {
    executor.shutdown()
  }
}


object Window {
  def slidingWindow[IN: ClassTag, OUT: ClassTag](
    slideDuration: Duration,
    windowDuration: Duration,
    allowedLatency: Duration = Duration(0, TimeUnit.MILLISECONDS)):
    SlideWindow[IN, OUT] ={
    new SlideWindow[IN, OUT](slideDuration, windowDuration, allowedLatency)
  }

  def tumblingWindow[IN: ClassTag, OUT: ClassTag](
    windowDuration: Duration,
    allowedLatency: Duration = Duration(0, TimeUnit.MILLISECONDS)): Unit ={
    slidingWindow[IN, OUT](windowDuration, windowDuration, allowedLatency)
  }
}
