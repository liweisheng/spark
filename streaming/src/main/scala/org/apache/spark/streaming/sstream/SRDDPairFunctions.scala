package org.apache.spark.streaming.sstream

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.pipeline.PipelineEvent
import org.apache.spark.streaming.window.{Window, WindowIdentifier}

import scala.reflect.ClassTag

class SRDDPairFunctions[K, V](self: SRDD[(K, V)])
    (implicit kt: ClassTag[K], vt: ClassTag[V])
  extends Logging with Serializable{

  def groupByKeyAndWindow[OUT: ClassTag](
      window: Window[(K, V), OUT],
      part: Partitioner,
      serializer: Serializer): SRDD[(WindowIdentifier, Iterator[OUT])] ={
    val splits = Array[Int](0)
    val splitAliases = Array[Any](0)
    val outputSelector = new OutputSelector[K, Int] {
      override def select(key: K): Array[Int] = Array(0)
    }

    val prev= self.internalRDD.asInstanceOf[RDD[PipelineEvent[_ <: Product2[K, V]]]]
    val shuffleId = prev.context.newShuffleId()
    val dep = new SplitDependency(prev, Array(part), serializer, splits, splitAliases, 0, outputSelector, shuffleId)

    val keyedRDD = new KeyedRDD[K, V](prev, part, dep )

    val windowRDD = new WindowRDD(keyedRDD, window)
    new SRDD[(WindowIdentifier, Iterator[OUT])](self.ssc, windowRDD)
  }

  def groupByKey(
    partitioner: Partitioner,
    serializer: Serializer = null): SRDD[(K, V)] = {
    require(partitioner != null, "partition can not be null")
    val partitioners = Array.fill(1)(partitioner)
    val splitsNames = Array.fill(1)("default")
    val outputSelector = new OutputSelector[K, String] {
      override def select(key: K): Array[String] = {
        Array.fill(1)("default")
      }
    }

    splitByKey(partitioners, splitsNames, outputSelector, serializer)(0)._2
  }

  def splitByKey(
    partitioners: Array[Partitioner],
    splitNames: Array[String],
    outputSelector: OutputSelector[K, String],
    userSpecifiedSerializer: Serializer = null): Array[(String, SRDD[(K, V)])] = {

    require(splitNames != null, "splitNames should not be null")
    require(splitNames.forall(_ != null), "splitNames should not contain null value")
    require(outputSelector != null, "outputSelector should not be null")
    require(partitioners.length == splitNames.length, s"partitioner length: ${partitioners.length}," +
      s" expected:${splitNames.length}")

    val splits = (0 to splitNames.length - 1).toArray
    val splitAliases = new Array[Any](splitNames.length)
    splitNames.zipWithIndex.map(nameIndex => splitAliases(nameIndex._2) = nameIndex._1)
    val prev= self.internalRDD.asInstanceOf[RDD[PipelineEvent[_ <: Product2[K, V]]]]
    val shuffleId = prev.context.newShuffleId()

    val ret = new Array[(String, SRDD[(K, V)])](splitNames.length)

    val serializer = if (userSpecifiedSerializer == null) SparkEnv.get.serializer else userSpecifiedSerializer
    splitNames.zipWithIndex.foreach(
      nameIndex => {
        val index = nameIndex._2
        val dep = new SplitDependency(prev, partitioners, serializer, splits,
          splitAliases, index, outputSelector, shuffleId)

        val keyedRDD = new KeyedRDD[K, V](prev, partitioners(index), dep)
        ret(index) = (nameIndex._1, new SRDD(self.ssc, keyedRDD))
      }
    )

    ret
  }
}
