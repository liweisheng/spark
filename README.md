# 1. 概要

这个分支主要基于Spark框架开发一种新的真正的流处理引擎， 已有的Spark streamming和Structured Streamming的都是基于mini-batch实现的，这种实现方式使得基于流的窗口操作， CEP（Complex Event Processing）实现不够灵活。

这里我试图基于spark实现一种真正的实时流处理引擎，而非mini-batch模式。实现过程中核心需要解决的问题有：

1. Spark的DAGScheduler，我们知道spark会将RDD DAG按照Shuffle Dependency划分为顺序依赖的Stage，DAGScheduler会将Stage转换为Task去提交运行，一个Stage的能够被提交运行的前提上一个Stage运行结束，这种方式在批处理模式下没有问题，但是在实时计算的情况下，显然上一个Stage是不可能结束的，Spark Streamming的解决方式就是把流数据切分成mini-batch，然后提交一个个mini-job。所以这里第一步需要修改DAGScheduler，使其在上游Stage产生数据之后，就应该通知到DAGScheduler让其启动下游Stage。

2. Shuffle的过程，Spark的shuffle完全是为批处理设计的，如果存在A <- B 之间的Shuffle Dependency， 那么必须等到A生成所有数据（也就是Shuffle Write），然后B才能读取数据（也就是Shuffle Read），数据不是实时流动。因此这里需要引入一种新的dependency，B能够实时的pull A的数据

3. split，分流操作，这里split是一种新的transformation，使得数据在该transformation流向不同分支，后续做不同的处理，类似下面这种操作：

   ```Scala
   val sourceRDD = sc.source()...  
   val (subRDD1, subRDD2) = sourceRDD.split(subRDDNames, subRDDSelector)
   subRDD1.map(...).foreach
   subRDD2.filer(...).foreach
   ...
   ```

   Spark批处理程序简介的支持这种操作:

   ```Scala
   val sourceRDD = sc.source()...
   sourceRDD.map.foreach...
   sourceRDD.filter.foreach...
   ```

   当然这种方式其实是通过启动两个job来完成的， split操作希望在同一个job中完成

4. CEP，复杂事件处理（Complex Event Processing）， 目前Spark不支持

5. Low Watermark，目前Structured Streaming支持low watermark， 但是和它的窗口操作一样不灵活

6. 窗口操作，Spark Streamming时间窗口（滑动和翻滚窗口），缺点态度，比如滑动时间必须是job duration的整数倍，必须要靠记录历史RDD来实现窗口聚合等等

7. Checkpoint， Spark支持checkpoint的方式是通过提交一个新的job去实现的，另外Spark Streamming的checkpoint是直接将rdd序列化到文件实现的，改一下代码，重新编译一下就不能反序列化了

这里计划能够一步步解决这些问题。



# 2. 使用示例

## 2.1 简单的demo

这里一个简单的demo，源头不断产生"a", "b", "c" ，并将他们分别输出到三个文件中，如下：

```Scala
import java.io.FileWriter

import org.apache.spark.{HashPartitioner, OutputSelector, Partitioner, SparkContext}
import org.apache.spark.streaming.sstream.{EventtimeExtractor, OutputCollector, SStreamContext, SourceFunction}

object DemoSplit {
  
  // 源头数据产生通过自定义SourceFunction来实现
  val stringsGeneratorFunction = new SourceFunction[(String, Long)] {
    var abc = Array("a", "b", "c")
    var index = 0
    override def run(collector: OutputCollector[(String, Long)]): Unit = {
      while(true){
        // 输出（"a", currentTimestamp）这种数据
        collector.collect((abc(index % 3), System.currentTimeMillis()))
        index += 1
        Thread.sleep(10)
      }
    }

    override def toString: String = {"stringsGeneratorFunction"}
  }

  // 源头需要一个事件事件抽取器来获取事件时间
  val eventTimeExtractor = new EventtimeExtractor[(String, Long)] {
    override def extractEventTime(event: (String, Long)): Long = {
      event._2
    }
  }

  def main(args: Array[String]) = {
    val sc = new SparkContext()
    // 使用SStreamContext
    val ssc = new SStreamContext(sc)

    // 创建数据源RDD， stringsGeneratorFunction产生时间，eventTimeExtractor抽取时间
    val source = ssc.source[(String, Long)](Array(stringsGeneratorFunction), eventTimeExtractor)

    // split 操作产生多子管道， OutputSelector用于根据key选择发送的下游管道
    val outputSelector = new OutputSelector[String, String] {
      override def select(key: String): Array[String] = {
        if(key == "a"){
          Array("pipeline-a")
        } else if (key == "b") {
          Array("pipeline-b")
        } else {
          Array("pipeline-c")
        }
      }
    }

    val partitioners: Array[Partitioner] = Array.fill(3)(new HashPartitioner(1))
    val splitNames = Array("pipeline-a", "pipeline-b", "pipeline-c")
    
    // 这里split产生三个下游子管道："pipeline-a", "pipeline-b", "pipeline-c"， split操作会
    // 导致shuffle，可以为每一个子管道设置一个paritioner
    val splitedRDDs = source.splitByKey(partitioners, splitNames, outputSelector)
    
    // 创建输出文件
    val fileSinkA = new SplitFileSink("/Users/liweisheng/testSplit/file_a_" + System.currentTimeMillis())
    val fileSinkB = new SplitFileSink("/Users/liweisheng/testSplit/file_b_" + System.currentTimeMillis())
    val fileSinkC = new SplitFileSink("/Users/liweisheng/testSplit/file_c_" + System.currentTimeMillis())

    // 输出到文件
    splitedRDDs(0)._2.foreachStream( record => {
      fileSinkA.sink(record)
    })

    splitedRDDs(1)._2.foreachStream( record => {
      fileSinkB.sink(record)
    })

    splitedRDDs(2)._2.foreachStream( record => {
      fileSinkC.sink(record)
    })

    Thread.sleep(Long.MaxValue)
  }

}

class SplitFileSink(val filePath: String) extends Serializable{

  lazy val fileWriter: FileWriter = new FileWriter(filePath)


  def sink(line: (String, Long)) = {
    fileWriter.write(line.toString())
    fileWriter.write(" ")
    fileWriter.write((System.currentTimeMillis() - line._2).toString)
    fileWriter.write("\n")
    fileWriter.flush()
  }
}
```

## 2.2 使用CEP

如下：

1. 创建pattern

   ```Java
   public class PatternBuilder {
       public static Pattern<String> buildPattern() {
           Pattern<String> pattern = Pattern.<String>start("A").where(new IterableCondition<String>() {
               public boolean match(String value, Iterator<String> values) {
                   return value.startsWith("a");}
               }
           ).followedBy("B").where(new IterableCondition<String>() {
               public boolean match(String value, Iterator<String> values) {
                   return value.startsWith("b");
               }
           }).oneOrMore().notFollowedBy("C").where(new IterableCondition<String>() {
               public boolean match(String value, Iterator<String> values) {
                   return value.startsWith("c");
               }
           }).followedBy("D").where(new IterableCondition<String>() {
               public boolean match(String value, Iterator<String> values) {
                   return value.startsWith("d");
               }
           });

           return pattern;
       }
   }
   ```


2. 创建spark任务

   ```Scala
   import org.apache.spark.streaming.cep.nfa.Pattern
   import org.apache.spark.{HashPartitioner, OutputSelector, SparkContext}
   import org.apache.spark.streaming.sstream._
   import scala.collection.JavaConverters._

   import scala.util.Random

   object DemoCEP {
     val stringsGeneratorFunction = new SourceFunction[String] {
       var index = 0
       val prefix = "Test"
       val charArray = Array[String]("a", "b", "c", "d")
       lazy val randomIntGenerator = new Random()

       override def run(collector: OutputCollector[String]): Unit = {
         while(true){
           val nextInt = randomIntGenerator.nextInt(4)
           collector.collect((charArray(nextInt)))
           Thread.sleep(10)
         }
       }

       override def toString: String = {"stringsGeneratorFunction"}
     }

     val eventTimeExtractor = new EventtimeExtractor[String] {
       override def extractEventTime(event: String): Long = {
         System.currentTimeMillis()
       }
     }

     def main(args: Array[String]) = {
       val sc = new SparkContext()
       val ssc = new SStreamContext(sc)
       val sourceSRDD = ssc.source[String](Array(stringsGeneratorFunction), eventTimeExtractor)
       val pattern = PatternBuilder.buildPattern()
      val patternSRDD = sourceSRDD.pattern(pattern, new PatternProcessor[String, String] {
        override def processMatchedPattern(startTimestamp: Long, patterns: util.Map[String, util.List[String]]): String = {
          val eventsMatchA = patterns.get("A").asScala.mkString("A(", "," , ")")
          val eventsMatchB = patterns.get("B").asScala.mkString("B(", "," , ")")
          val eventsMatchD = patterns.get("D").asScala.mkString("D(", "," , ")")

          Array(eventsMatchA, eventsMatchB, eventsMatchD).mkString("{", ",", "}")
        }

        override def processTimeoutPattern(startTimestamp: Long, patterns: util.Map[String, util.List[String]]): String = {
          null.asInstanceOf[String]
        }
      })

      val fileSink = new FileSink("/Users/liweisheng/testSplit/demo-cep")
      patternSRDD.foreachStream(
        record => fileSink.sink(record, System.currentTimeMillis())
      )
    }}
   ```


**附**

cep的实现参考了flink，此外还参考了如下论文：

1. Efficient Pattern Matching over Event Streams

2. High-Performance Complex Event Processing over Streams


   ​

## 2.3 时间窗口

```Scala
package me.liws.testpipeline

import java.io.FileWriter
import java.text.SimpleDateFormat
import java.util.concurrent.TimeUnit

import org.apache.spark.{HashPartitioner, OutputSelector, SparkContext}
import org.apache.spark.streaming.sstream.{EventtimeExtractor, OutputCollector, SStreamContext, SourceFunction}
import org.apache.spark.streaming.window.{TriggerPolicy, Window, WindowComputation, WindowIdentifier}

import scala.concurrent.duration.Duration

object DemoWindow {
  val stringsGeneratorFunction = new SourceFunction[(String, Long)] {
    var index = 0
    val prefix = "Test"
    override def run(collector: OutputCollector[(String, Long)]): Unit = {
      while(true){
        val nextString = prefix + index
        index += 1
        collector.collect((nextString, System.currentTimeMillis()))
        Thread.sleep(10)
      }
    }

    override def toString: String = {"stringsGeneratorFunction"}
  }

  val eventTimeExtractor = new EventtimeExtractor[(String, Long)] {
    override def extractEventTime(event: (String, Long)): Long = {
      event._2
    }
  }

  def main(args: Array[String]) = {
    val sc = new SparkContext()
    val ssc = new SStreamContext(sc)


    val source = ssc.source[(String, Long)](Array(stringsGeneratorFunction), eventTimeExtractor)

    // 创建滑动窗口
    val slideWindow = Window.slidingWindow[(String, Long), (Int, Long, Long)](
      Duration(1000, TimeUnit.MILLISECONDS),
      Duration(4000, TimeUnit.MILLISECONDS))
      .triggerBy(TriggerPolicy.ByTime)
      // 窗口内数据计算逻辑，这里统计窗口数据个数
      .computation(WindowComputation.newWindowComputation[(String, Long)]().mapIterator(iterator => {
        var count = 0;
        var head, last: Long = null.asInstanceOf[Long]
        try {
          while (iterator.hasNext) {
            val next = iterator.next()
            if (head == null.asInstanceOf[Long]) {
              head = next._2
            }
            last = next._2
            count += 1
          }
        } catch {
          case e => {
            e.printStackTrace()
            e.printStackTrace()
          }
        }
        (count, head, last)
      }))

    // 创建一个window srdd
    val slideWindowSRDD = source.groupByKeyAndWindow(slideWindow, new HashPartitioner(1), null)

    // 输出到文件
    val fileSink = new DemoFileSink("/Users/liweisheng/testWindow/window-" + System.currentTimeMillis())

    slideWindowSRDD.foreachStream( record => {
      fileSink.sink(record)
    })

    Thread.sleep(Long.MaxValue)
  }

}

class DemoFileSink(
  val filePath: String) extends Serializable{

  lazy val fileWriter: FileWriter = new FileWriter(filePath)


  def sink(record: (WindowIdentifier,Iterator[(Int, Long, Long)])) = {
    fileWriter.write(record._1.toString)
    fileWriter.write(": ")
    fileWriter.write(s"${record._2.toArray.mkString("[", ",", "]")}")
    fileWriter.write("\n")
    fileWriter.flush()
  }
}
```

**附**

关于窗口参考文章：

1. MillWheel: Fault-Tolerant Stream Processing at Internet Scale
2. The Dataflow Model: A Practical Approach to Balancing Correctness, Latency, and Cost in Massive-Scale, Unbounded, Out-of-Order Data Processing	

# 未来计划

1. 还有很多bug待fix
2. count窗口和session窗口
3. 支持checkpoint和watermark



