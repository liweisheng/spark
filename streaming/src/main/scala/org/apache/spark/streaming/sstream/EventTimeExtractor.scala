package org.apache.spark.streaming.sstream

trait EventtimeExtractor[T] extends Serializable{
  def extractEventTime(event: T): Long
}
