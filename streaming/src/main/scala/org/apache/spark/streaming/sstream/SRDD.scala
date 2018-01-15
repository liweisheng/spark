package org.apache.spark.streaming.sstream

import org.apache.spark.rdd.RDD
import org.apache.spark.shuffle.pipeline.PipelineEvent

import scala.reflect.ClassTag

abstract class SRDD[T: ClassTag] private[spark] (
    internalRDD: RDD[PipelineEvent[T]]) {
  def processCheckPoint(): PipelineEvent[T]

  def processWaterMark(): PipelineEvent[T]


}
