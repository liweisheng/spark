package org.apache.spark.streaming.sstream

import org.apache.spark.rdd.{PairRDDFunctions, RDD}
import org.apache.spark.shuffle.pipeline.PipelineEvent
import org.apache.spark.streaming.cep.nfa.Pattern

import scala.reflect.ClassTag

class SRDD[T: ClassTag] private[spark] (
  var internalRDD: RDD[PipelineEvent[T]]) {
  def processCheckPoint(pipelineEvent: PipelineEvent[T]) = {
  }

  def processWaterMark(pipelineEvent: PipelineEvent[T]) = {
  }


  def map[U: ClassTag](f: T => U): SRDD[U] = {
    val mapFunc = (e: PipelineEvent[T]) => {
      var ret: PipelineEvent[U] = null
      if (e.isCheckpoint) {
        processCheckPoint(e)
        ret = PipelineEvent.convertNonDataEvent[U](e)
      } else if (e.isWaterMark) {
        processWaterMark(e)
        ret = PipelineEvent.convertNonDataEvent[U](e)
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
        ret = Iterator(PipelineEvent.convertNonDataEvent[U](pipelineEvent))
      } else if(pipelineEvent.isWaterMark) {
        processWaterMark(pipelineEvent)
        ret = Iterator(PipelineEvent.convertNonDataEvent[U](pipelineEvent))
      } else {
        ret = f(pipelineEvent.data).map(u => PipelineEvent.dataEvent(u, pipelineEvent.eventTime))
      }
      ret
    }

    val newRDD = internalRDD.flatMap(flatMapFunc)
    new SRDD[U](newRDD)
  }

  def pattern[U: ClassTag](
    pattern: Pattern[T],
    patternProcessor: PatternProcessor[T, U]): SRDD[U] = {
    val patternRDD = new PatternRDD[T, U](internalRDD, pattern, patternProcessor)
    new SRDD[U](patternRDD)
  }
}

object RDD {
  implicit def srddToPairSRDDFunctions[K, V](srdd: SRDD[(K,V)])
    (implicit kt: ClassTag[K], vt: ClassTag[V]): SRDDPairFunctions[K, V] = {
    new SRDDPairFunctions(srdd)
  }
}
