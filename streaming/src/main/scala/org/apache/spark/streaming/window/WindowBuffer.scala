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

import java.util.concurrent.TimeUnit

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.Duration

case class LatticeIdentifier(
  val startTimestamp: Long,
  val size: Long)


class Lattice[T](
  val latticeId: LatticeIdentifier){
  val internalBuffer: ArrayBuffer[T] = ArrayBuffer.empty

  def insert(e: T) = {
    internalBuffer += e
  }
}

abstract class WindowBuffer[T] extends Serializable{
  def triggerNext(upperEventTime: Long): Seq[(WindowIdentifier, Iterable[T])]

  def insert(data: T, timestamp: Long): Seq[(WindowIdentifier, Iterable[T])]

  def windowsContainTimestamp(timestamp: Long): Seq[WindowIdentifier]
}

class TimeWindowBuffer[T](
  val slideDuration: Duration,
  val windowDuration: Duration,
  val allowedLatency: Duration = Duration.create(0, TimeUnit.MILLISECONDS)) extends WindowBuffer[T] {
  require(windowDuration >= slideDuration,
    s"Invalid, windowDuration:$windowDuration is less then slideDuration:$slideDuration")
  require(allowedLatency.toMillis >= 0,
    s"Invalid, allowedLatency:$allowedLatency is less then 0(millis)")

  private[this] val latticeSize = greatestCommonDivisor(windowDuration.toMillis, slideDuration.toMillis)

  private[this] var lastLatticeIdOfLastTrigger: LatticeIdentifier = null

  @volatile
  private[this] var lastTriggerTimestamp = Long.MinValue

  private[this] var internalLatticeBuffer: ArrayBuffer[Lattice[T]] = ArrayBuffer.empty

  private[this] val garbageCollectIntervalMillis = (windowDuration + allowedLatency).toMillis

  private[this] var latestGarbageCollectMillis = Long.MinValue

  private[this] var windowTriggerTimeMap: scala.collection.mutable.LinkedHashMap[WindowIdentifier, Long] =
    scala.collection.mutable.LinkedHashMap.empty

  override def triggerNext(upperEventTime: Long = Long.MaxValue): List[(WindowIdentifier, Iterable[T])] = {
    if(internalLatticeBuffer.isEmpty) {
      return List.empty
    }

    var ret: List[(WindowIdentifier, Iterable[T])] = List.empty

    if(lastLatticeIdOfLastTrigger == null){
      val headLatticId = internalLatticeBuffer.head.latticeId
      var nextLatticeId = nextNthLattice(headLatticId, (slideDuration.toMillis / latticeSize).toInt)
      val lastLatticeId = internalLatticeBuffer.last.latticeId
      while(compareLatticeId(nextLatticeId, lastLatticeId) <= 0) {
        val previousNLattices = previousNLattice(nextLatticeId,
          (windowDuration.toMillis / latticeSize).toInt)

        val window = buildWindow(previousNLattices, nextLatticeId)
        if(window.isDefined) {
          ret :+= window.get
          windowTriggerTimeMap += (window.get._1 -> System.currentTimeMillis())
        }

        lastLatticeIdOfLastTrigger = nextLatticeId
        lastTriggerTimestamp = System.currentTimeMillis()
        nextLatticeId = nextNthLattice(lastLatticeIdOfLastTrigger, (slideDuration.toMillis / latticeSize).toInt)
      }
    }else{
      val currentTimestamp = System.currentTimeMillis()
      if(currentTimestamp - lastTriggerTimestamp >= slideDuration.toMillis) {

        val windowCount = (currentTimestamp - lastTriggerTimestamp) / slideDuration.toMillis
        var i = 1L
        while(i <= windowCount){
          val nextLastLatticeId = nextNthLattice(lastLatticeIdOfLastTrigger,
            (slideDuration.toMillis / latticeSize).toInt)

          if(nextLastLatticeId.startTimestamp + latticeSize > upperEventTime){
            i = windowCount + 1
          }else{
            val previousNLattices = previousNLattice(nextLastLatticeId, (windowDuration.toMillis / latticeSize).toInt)
            val window = buildWindow(previousNLattices,
              nextLastLatticeId)
            if(window.isDefined){
              ret :+= window.get
              windowTriggerTimeMap += (window.get._1 -> System.currentTimeMillis())
            }
            lastLatticeIdOfLastTrigger = nextLastLatticeId
            lastTriggerTimestamp = System.currentTimeMillis()
            i += 1
          }

        }
      }
    }
    ret
  }

  override def windowsContainTimestamp(timestamp: Long): Seq[WindowIdentifier] = {
    computeWindowsContainTime(timestamp)
  }

  def insert(data: T, timestamp: Long): Seq[(WindowIdentifier, Iterable[T])]= {
    val latticeId = computeLattice(timestamp)
    val floorLatticeID = ceilOfLattice(latticeId)

    var reTriggerWindows = Seq.empty[(WindowIdentifier, Iterable[T])]

    if(floorLatticeID.isEmpty) {
      val lattice = new Lattice[T](latticeId)
      lattice.insert(data)
      if(internalLatticeBuffer.isEmpty){
        internalLatticeBuffer.append(lattice)
      }else{
        internalLatticeBuffer.prepend(lattice)
      }
    }else{
      val index = searchLattice(floorLatticeID.get)
      val floorLattice = internalLatticeBuffer(index)
      val compare = compareLatticeId(floorLattice.latticeId, latticeId)
      if(compare == 0){
        floorLattice.insert(data)
      }else if(compare < 0){
        val lattice = new Lattice[T](latticeId)
        lattice.insert(data)
        internalLatticeBuffer.insert(index + 1, lattice)
      }else{
        val lattice = new Lattice[T](latticeId)
        lattice.insert(data)
        internalLatticeBuffer.insert(index, lattice)
      }
    }


    if(latestGarbageCollectMillis == Long.MinValue){
      latestGarbageCollectMillis = timestamp
    }

    if(timestamp - latestGarbageCollectMillis > garbageCollectIntervalMillis) {
      latestGarbageCollectMillis = timestamp
      dropUselessLattices
    }

    if(lastLatticeIdOfLastTrigger != null && compareLatticeId(latticeId, lastLatticeIdOfLastTrigger) <= 0){
      reTriggerWindows = computeWindowsShouldTriggerAgain(timestamp)
    }

    reTriggerWindows
  }

  def currentLattices(): Seq[(LatticeIdentifier, Int)] = {
    internalLatticeBuffer.map(l => (l.latticeId, l.internalBuffer.foldLeft(0)((x, _) => x + 1)))
  }

  def getLatestTriggerWindowId(): TimeWindowIdentifier = {
    val startTime = lastLatticeIdOfLastTrigger.startTimestamp + latticeSize - windowDuration.toMillis
    new TimeWindowIdentifier(startTime, windowDuration)
  }

  def getLatestTriggerWindowTime(): Long = {
    getLatestTriggerWindowId().startTimestamp + getLatticeSize
  }

  def getLatticeSize = latticeSize

  def computeWindowTriggerTime(windowIdentifier: TimeWindowIdentifier): Long = {
    val endTime = windowIdentifier.startTimestamp + windowDuration.toMillis
    val lastWindowEndTime = lastLatticeIdOfLastTrigger.startTimestamp + latticeSize
    val slideCount = (lastWindowEndTime - endTime) / slideDuration.toMillis
    lastTriggerTimestamp - slideCount * slideDuration.toMillis
  }

  def shouldReserve(windowIdentifier: TimeWindowIdentifier): Boolean = {
    if(lastLatticeIdOfLastTrigger == null){
      true
    }else{
      val lastTriggerWindowEndTime = lastLatticeIdOfLastTrigger.startTimestamp + latticeSize
      if(lastTriggerWindowEndTime > windowIdentifier.startTimestamp + windowDuration.toMillis){
        false
      }else {
        true
      }
    }
  }

  def computeWindowsShouldTriggerAgain(timestamp: Long): Seq[(WindowIdentifier, Iterable[T])] = {
    if(allowedLatency.toMillis <= 0){
      Seq.empty
    }else{
      val windowCandidates = windowsContainTimestamp(timestamp)
      val windowsShouldTriggerAgain = windowCandidates.filter(w => shouldReserve(w.asInstanceOf[TimeWindowIdentifier]))
      val windows = windowsShouldTriggerAgain.map(
        w => {
          val timeWindow = w.asInstanceOf[TimeWindowIdentifier]
          val windowEndTime = timeWindow.startTimestamp + timeWindow.duration.toMillis
          val lastLatticeIdOfTW = new LatticeIdentifier(windowEndTime - latticeSize, latticeSize)
          val latticesOfTW = previousNLattice(lastLatticeIdOfTW,(timeWindow.duration.toMillis / latticeSize).toInt)
          buildWindow(latticesOfTW, lastLatticeIdOfTW)
        }).filter(_.isDefined).map(_.get)
      windows
    }
  }

  private[this] def dropUselessLattices(): Unit ={
    val newBuffer = internalLatticeBuffer.filter(
      lattice => {
        val startTime = lattice.latticeId.startTimestamp
        val windowsContainLattice = windowsContainTimestamp(startTime)
        val anyWindowsContainTime = if(windowsContainLattice.isEmpty){
          lastLatticeIdOfLastTrigger == null
        }else{
          val latestWindow = windowsContainLattice.last
          val shouldDrop = !shouldReserve(latestWindow.asInstanceOf[TimeWindowIdentifier])
          !shouldDrop
        }
        anyWindowsContainTime
      })
    println(s"internalBufferSize: ${internalLatticeBuffer.size}")

    if(internalLatticeBuffer.size != newBuffer.size){
      println(s"Drop: ${internalLatticeBuffer.size - newBuffer.size}, lastTriggerTime:${lastLatticeIdOfLastTrigger}, " +
        s"currentHead:${if(!newBuffer.isEmpty) newBuffer.head.latticeId}, currentTail:${newBuffer.last.latticeId}")
    }
    internalLatticeBuffer = newBuffer
  }

  private[this] def ceilOfLattice(latticeId: LatticeIdentifier): Option[LatticeIdentifier] = {
    if(internalLatticeBuffer.isEmpty){
      None
    }else if(compareLatticeId(latticeId, internalLatticeBuffer.last.latticeId) >= 0){
      Some(internalLatticeBuffer.last.latticeId)
    }else if(compareLatticeId(latticeId, internalLatticeBuffer.head.latticeId) < 0 ){
      None
    }else{
      Some(internalLatticeBuffer.filter(i => compareLatticeId(latticeId,
        i.latticeId) >= 0).last.latticeId)
    }
  }

  // include current lattice of param
  private[this] def previousNLattice(latticeIdentifier: LatticeIdentifier, n: Int): Seq[Lattice[T]] = {
    val floorLatticeId = ceilOfLattice(latticeIdentifier)
    if(floorLatticeId.isDefined){
      val index = searchLattice(floorLatticeId.get)
      if(index < 0){
        Seq.empty
      }else{
        internalLatticeBuffer
          .filter(l => l.latticeId.startTimestamp >= latticeIdentifier.startTimestamp - latticeSize * (n - 1)
            && l.latticeId.startTimestamp <= latticeIdentifier.startTimestamp)
      }
    }else{
      Seq.empty
    }
  }

  private[this] def compareLatticeId(left: LatticeIdentifier, right: LatticeIdentifier): Int = {
    if(left.startTimestamp == right.startTimestamp){
      0
    }else if(left.startTimestamp < right.startTimestamp){
      -1
    }else{
      1
    }
  }

  private[this] def computeLattice(timestamp: Long): LatticeIdentifier = {
    val startTimestamp = (timestamp / latticeSize) * latticeSize
    LatticeIdentifier(startTimestamp, latticeSize)
  }

  private[this] def nextNthLattice(lattice: LatticeIdentifier, n: Int): LatticeIdentifier = {
    LatticeIdentifier(lattice.startTimestamp + n * lattice.size, lattice.size)
  }

  private[this] def previousNthLattice(lattice: LatticeIdentifier, n: Int): LatticeIdentifier = {
    LatticeIdentifier(lattice.startTimestamp - n * lattice.size, lattice.size)
  }

  private[this] def searchLattice(expectedLatticeId: LatticeIdentifier): Int = {
    var start = 0
    var end = internalLatticeBuffer.size - 1
    while(start <= end) {
      val mid = (end - start) / 2 + start
      if(compareLatticeId(internalLatticeBuffer(mid).latticeId, expectedLatticeId) == 0){
        return mid
      }

      if(compareLatticeId(internalLatticeBuffer(mid).latticeId, expectedLatticeId) > 0){
        end = mid - 1
      }else{
        start = mid + 1
      }
    }
    -1
  }

  private[this] def greatestCommonDivisor(divisor: Long, dividend: Long): Long = {
    var divisorTmp = divisor
    var dividendTmp = dividend
    var remainder = divisor % dividend
    while(remainder != 0) {
      divisorTmp = dividendTmp
      dividendTmp = remainder
      remainder = divisorTmp % dividendTmp
    }
    dividendTmp
  }

  private[this] def buildWindow(previousNLattices: Seq[Lattice[T]],  latticeIdentifier: LatticeIdentifier)
  : Option[(WindowIdentifier, Iterable[T])] = {
    if(previousNLattices.isEmpty) {
      None
    }else{
      val windowEndTimestamp = latticeIdentifier.startTimestamp + latticeSize
      val windowIdentifer = TimeWindowIdentifier(windowEndTimestamp - windowDuration.toMillis, windowDuration)
      val buffer: ArrayBuffer[T] = new ArrayBuffer()
      previousNLattices.flatMap(_.internalBuffer).copyToBuffer(buffer)
      Some((windowIdentifer, buffer))
    }
  }

  private[this] def computeWindowsContainTime(timestamp: Long): Seq[TimeWindowIdentifier] = {
    if(lastLatticeIdOfLastTrigger == null){
      return Seq.empty
    }
    var rst: Seq[TimeWindowIdentifier] = Seq.empty
    val lastWindowEndTime = lastLatticeIdOfLastTrigger.startTimestamp + latticeSize
    val slideCount = Math.floor((lastWindowEndTime - timestamp).toDouble / slideDuration.toMillis.toDouble).toInt
    val earliestWindowWithTime = new TimeWindowIdentifier(
      lastWindowEndTime - slideCount * slideDuration.toMillis - windowDuration.toMillis, windowDuration)

    rst +:=  earliestWindowWithTime

    var nextStartTime = earliestWindowWithTime.startTimestamp + slideDuration.toMillis
    while(nextStartTime <= timestamp) {
      val nextWindow = new TimeWindowIdentifier(nextStartTime, windowDuration)
      rst :+= nextWindow
      nextStartTime += slideDuration.toMillis
    }
    rst
  }
 }

