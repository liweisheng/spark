package org.apache.spark.streaming.sstream

import org.apache.spark.HashPartitioner
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.shuffle.pipeline.PipelineEvent
import org.apache.spark.streaming.window.{Window, WindowIdentifier}

import scala.reflect.ClassTag

class SRDDPairFunctions[K, V](self: SRDD[(K, V)])
    (implicit kt: ClassTag[K], vt: ClassTag[V])
  extends Logging with Serializable{

  def groupByKeyAndWindow[OUT: ClassTag](
      window: Window[(K, V), OUT],
      numPartitions: Int): SRDD[(WindowIdentifier, Iterator[OUT])] ={
    val keyedRDD = new KeyedRDD[K, V](self.internalRDD.asInstanceOf[RDD[PipelineEvent[_ <: Product2[K, V]]]],
      new HashPartitioner(numPartitions))

    val windowRDD = new WindowRDD(keyedRDD, window)
    new SRDD[(WindowIdentifier, Iterator[OUT])](windowRDD)
  }
}
