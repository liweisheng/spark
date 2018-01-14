package org.apache.spark.shuffle.pipeline

import java.io.InputStream
import java.lang.Long
import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue}

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.network.shuffle.{PipelineSegmentFetchingListener, ShuffleClient}
import org.apache.spark.storage.{BlockManagerId, PipelineManagerId}


/**
  * Fetch data buffers from mappers, create an input stream from every received data buffer, then iterate these input streams.
  * This class can be used both in streaming and batch processing.
  *
  * @author liweisheng
  * */
private[spark] final class PipelineSegmentFetcherIterator(
    taskContext: TaskContext,
    shuffleClient: ShuffleClient,
    pipelineWithAddress: Seq[(BlockManagerId, Seq[PipelineManagerId])],
    subPipelineIndex: Int,
    reduceId: Int)
  extends Iterator[InputStream] with Logging{

  private[this] val EMPTY_QUEUE = new LinkedBlockingQueue[FetchResult]()

  @volatile
  private[this] var isStart = false

  @volatile
  private[this] var currentResult: SuccessFetchResult = null

  private[this] val pipelineEndMark: scala.collection.mutable.Map[String, Boolean] =
    scala.collection.mutable.Map.empty

  private[this] val pipeline2Buffer: scala.collection.mutable.Map[String, BlockingQueue[FetchResult]] =
    scala.collection.mutable.Map.empty[String, BlockingQueue[FetchResult]]

  private[this] val pipelineManagerIds: scala.collection.mutable.Queue[String] = scala.collection.mutable.Queue.empty

  private[this] def nextPipeline(): String = {
    val next = pipelineManagerIds.dequeue()
    pipelineManagerIds.enqueue(next)
    next
  }

  pipelineWithAddress.flatMap(_._2).foreach(
    pipelineManagerId => {
      pipeline2Buffer.put(pipelineManagerId.name, new LinkedBlockingQueue[FetchResult]())
      pipelineEndMark.put(pipelineManagerId.name, false)
      pipelineManagerIds.enqueue(pipelineManagerId.name)
    }
  )

  start()

  def releaseCurrentResult = {
    if(currentResult != null){
      currentResult.buf.release()
    }
    currentResult = null
  }

  private[this] def start(): Unit =  {
    if(!isStart){
      synchronized{
        if(!isStart){
          for(
            (blockManagerId, pipelineManagedIds) <- pipelineWithAddress;
            pipelineManagerId <- pipelineManagedIds
          ) {
            shuffleClient.fetchPipelineSegment(
              blockManagerId.host,
              blockManagerId.port,
              pipelineManagerId.name,
              subPipelineIndex,
              reduceId,
              Long.MIN_VALUE,
              new PipelineSegmentFetchingListener {
                override def onPipelineSegmentFetchSuccess(pipelineManagerId: String, fetchId: Long, data: ManagedBuffer) = {
                  PipelineSegmentFetcherIterator.this.synchronized {
                    data.retain()
                    pipeline2Buffer.getOrElseUpdate(pipelineManagerId, new LinkedBlockingQueue[FetchResult]())
                      .put(new SuccessFetchResult(data))
                  }
                }

                override def onPipelineSegmentFetchFailure(pipelineManagedId: String, fetchId: Long, t: Throwable) = {
                  PipelineSegmentFetcherIterator.this.synchronized{
                    logError(s"Failed to get from ${blockManagerId.host}:${blockManagerId.port}, pipelineManagerId:$pipelineManagedId")
                    pipeline2Buffer.getOrElseUpdate(pipelineManagedId, new LinkedBlockingQueue[FetchResult]())
                      .put(new FailureFetchResult(t))
                  }
                }

                override def onPipelineEnd(pipelineManagerId: String, fetchId: Long): Unit = {
                  pipelineEndMark(pipelineManagerId) = true
                }
              }
            )
          }
          isStart = true
        }
      }
    }
  }

  override def hasNext: Boolean = !pipelineEndMark.values.exists(_ == false)

  override def next(): InputStream = {
    if(!hasNext){
      throw new NoSuchElementException
    }

    var result: FetchResult = null
    var inputStream: InputStream = null

    while(result == null){
      val dataQueue = pipeline2Buffer.get(nextPipeline())

      result = dataQueue.getOrElse(EMPTY_QUEUE).poll()

      result match {
        case r @ SuccessFetchResult(buf) =>
          inputStream = buf.createInputStream()
        case FailureFetchResult(e) =>
          //TODO: process failure, we shall remove pipelineManagerId here? now we keep it.
          result = null
      }
    }

    currentResult = result.asInstanceOf[SuccessFetchResult]
    new BufferReleasingInputStream(inputStream, this)
  }
}

private class BufferReleasingInputStream(
    private val delegate: InputStream,
    private val iterator: PipelineSegmentFetcherIterator)
  extends InputStream {
  private[this] var closed = false

  override def read(): Int = delegate.read()

  override def close(): Unit = {
    if (!closed) {
      delegate.close()
      iterator.releaseCurrentResult
      closed = true
    }
  }

  def skip(n: Long): Long = delegate.skip(n)

  override def available(): Int = delegate.available()

  override def mark(readlimit: Int): Unit = delegate.mark(readlimit)

  override def markSupported(): Boolean = delegate.markSupported()

  override def read(b: Array[Byte]): Int = delegate.read(b)

  override def read(b: Array[Byte], off: Int, len: Int): Int = delegate.read(b, off, len)

  override def reset(): Unit = delegate.reset()
}

private[spark] sealed trait FetchResult

private[spark] case class SuccessFetchResult(
    buf: ManagedBuffer) extends FetchResult

private[spark] case class FailureFetchResult(
    e: Throwable) extends FetchResult