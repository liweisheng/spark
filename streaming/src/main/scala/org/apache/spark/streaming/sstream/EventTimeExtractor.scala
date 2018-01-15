package org.apache.spark.streaming.sstream

trait EventTimeExtractor[T] {
  def extractEventTime(event: T): Long
}
