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

import java.io.{EOFException, InputStream, OutputStream}

import org.apache.spark.serializer.{SerializationStream, SerializerInstance}
import org.apache.spark.util.NextIterator


private[spark] class PipelineEventSerDe[K, V](
  val serializerInstance: SerializerInstance) {

  def serialize(eventIter: Iterator[PipelineEvent[Product2[K, V]]], outputStream: OutputStream) = {
    val objOut = serializerInstance.serializeStream(outputStream)

    try {
      while (eventIter.hasNext) {
        val e = eventIter.next
        objOut.writeObject(e.eventType)
        objOut.writeObject(e.eventTime)
        if (e.isData) {
          write(e.data._1, e.data._2, objOut)
        } else {
          objOut.writeObject(e.nonData)
        }
      }

      objOut.flush()
    } finally{
      if(objOut != null){
        objOut.close()
      }
    }
  }

  def deserialize(inputStream: InputStream): Iterator[PipelineEvent[Product2[Any, Any]]] = {
    val objIn = serializerInstance.deserializeStream(inputStream);

    return new NextIterator[PipelineEvent[Product2[Any, Any]]] {
    override protected def close() = {
      objIn.close()
    }

      override protected def getNext() = {
        try {
          val eventType = objIn.readObject().asInstanceOf[PipelineEvent.EvenType]
          val eventTime = objIn.readObject[Long]().asInstanceOf[Long]
          val pipelineEvent = if(eventType == PipelineEvent.DATA){
            val k = objIn.readKey[Any]()
            val v = objIn.readValue[Any]()
            new PipelineEvent(eventType, new Tuple2[Any, Any](k, v).asInstanceOf[Product2[Any, Any]], null, eventTime)
          } else {
            val nonData = objIn.readObject[AnyRef]()
            new PipelineEvent(eventType, (null, null).asInstanceOf[Product2[Any, Any]], nonData, eventTime)
          }

          pipelineEvent
        } catch {
          case eof: EOFException =>
            finished = true
            null
        }
      }
    }
  }

  private[this] def write(key: Any, value: Any, outputStream: SerializationStream) = {
    outputStream.writeKey(key)
    outputStream.writeValue(value)
  }
}

