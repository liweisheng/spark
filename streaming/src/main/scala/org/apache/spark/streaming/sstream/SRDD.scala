package org.apache.spark.streaming.sstream

import org.apache.spark.rdd.RDD
import org.apache.spark.shuffle.pipeline.PipelineEvent

import scala.reflect.ClassTag

class SRDD[T: ClassTag] private[spark] (
    internalRDD: RDD[PipelineEvent[T]]) {
  def processCheckPoint(pipelineEvent: PipelineEvent[T]) = {
  }

  def processWaterMark(pipelineEvent: PipelineEvent[T]) = {
  }


  def map[U: ClassTag](f: T => U): SRDD[U] = {
    val mapFunc = (e: PipelineEvent[T]) => {
      var ret: PipelineEvent[U] = null
      if (e.isCheckpoint) {
        processCheckPoint(e)
        ret = new PipelineEvent[U](
          e.eventType,
          null.asInstanceOf[U],
          e.nonData,
          e.eventTime)
      } else if (e.isWaterMark) {
        processWaterMark(e)
        ret = new PipelineEvent[U](
          e.eventType,
          null.asInstanceOf[U],
          e.nonData,
          e.eventTime)
      } else {
        val processedData = f(e.data)
        ret = new PipelineEvent[U](
          e.eventType,
          processedData,
          e.nonData,
          e.eventTime)
      }
      ret
    }

    val newRDD = internalRDD.map[PipelineEvent[U]](mapFunc)
    new SRDD[U](newRDD)
  }

  def filter(f: T => Boolean): SRDD[T] = {
    val filterFunc = (e: PipelineEvent[T]) => {
      var ret: Boolean = true
      if(e.isCheckpoint) {
        processCheckPoint(e)
        ret = true
      } else if(e.isWaterMark) {
        processWaterMark(e)
        ret = true
      } else{
        ret = f(e.data)
      }
      ret
    }

    val newRDD = internalRDD.filter(filterFunc)
    new SRDD[T](newRDD)
  }

  def flatMap[U: ClassTag](f: T => TraversableOnce[U]): SRDD[U] = {
    val flatMapFunc = (pipelineEvent: PipelineEvent[T]) => {
      var ret: TraversableOnce[PipelineEvent[U]] = null
      if(pipelineEvent.isCheckpoint) {
        processCheckPoint(pipelineEvent)
        ret = Iterator(new PipelineEvent[U](
          pipelineEvent.eventType,
          null.asInstanceOf[U],
          pipelineEvent.nonData,
          pipelineEvent.eventTime))
      } else if(pipelineEvent.isWaterMark) {
        processWaterMark(pipelineEvent)
        ret = Iterator(new PipelineEvent[U](
          pipelineEvent.eventType,
          null.asInstanceOf[U],
          pipelineEvent.nonData,
          pipelineEvent.eventTime))
      } else {
        ret = f(pipelineEvent.data).map(u => new PipelineEvent[U](
          pipelineEvent.eventType,
          u,
          pipelineEvent.nonData,
          pipelineEvent.eventTime
        ))
      }
      ret
    }

    val newRDD = internalRDD.flatMap(flatMapFunc)
    new SRDD[U](newRDD)
  }
}
