/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.scala.examples.socket

import org.apache.flink.api.common.functions.{FoldFunction, ReduceFunction}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable

/**
  * Implements a streaming windowed version of the "WordCount" program.
  *
  * This program connects to a server socket and reads strings from the socket.
  * The easiest way to try this out is to open a text sever (at port 12345)
  * using the <i>netcat</i> tool via
  * <pre>
  * nc -l 12345
  * </pre>
  * and run this example with the hostname and the port as arguments..
  */
object SocketSessionWindow {

  /** Main program method */
  def main(args: Array[String]): Unit = {

    val time = System.currentTimeMillis()

    print("time:" + time)

    // the host and the port to connect to
    var hostname: String = "192.168.142.145"
    var port: Int = 9999

    /* try {
       val params = ParameterTool.fromArgs(args)
       hostname = if (params.has("hostname")) params.get("hostname") else "localhost"
       port = params.getInt("port")
     } catch {
       case e: Exception => {
         System.err.println("No port specified. Please run 'SocketWindowWordCount " +
           "--hostname <hostname> --port <port>', where hostname (localhost by default) and port " +
           "is the address of the text server")
         System.err.println("To start a simple text server, run 'netcat -l <port>' " +
           "and type the input text into the command line")
         return
       }
     }*/

    // get the execution environment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    // get input data by connecting to the socket
    val text = env.socketTextStream(hostname, port, '\n')

    /* input
       .keyBy(<key selector>)
         .window(EventTimeSessionWindows.withGap(Time.minutes(10)))
         .<windowed transformation>(<window function>*/

    // parse the data, group it, window it, and aggregate the counts 
    val input = text
      .map { w => {
        val vs = w.split("\\s")
        new SystemSessionCount(vs(0), vs(1), time + vs(2).toLong, 1)
      }
      }
    /*.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[SystemSessionCount] {
      var currentMaxTimestamp = 0L
      val maxOutOfOrderness = 10000L

      override def getCurrentWatermark: Watermark = {
        new Watermark(currentMaxTimestamp - maxOutOfOrderness)
      }

      override def extractTimestamp(t: SystemSessionCount, l: Long): Long = {
        val timestamp = t.timestamp
        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp)
        timestamp
      }
    })*/


    val windowedStream = input.keyBy("system")
      //.window(EventTimeSessionWindows.withGap(Time.seconds(15)))
      .window(ProcessingTimeSessionWindows.withGap(Time.seconds(15)))

    windowedStream.apply(new WindowFunction[SystemSessionCount, (String, Long), Tuple, TimeWindow] {
      override def apply(key: Tuple, window: TimeWindow, input: Iterable[SystemSessionCount], out: Collector[(String, Long)]): Unit = {
        var sessionSet = new mutable.HashSet[String]()
        input.foreach(e => {
          sessionSet += e.sessionId
        })
        out.collect(key.getField(0), sessionSet.size)
      }
    })
    //.print()
    //.writeAsText("d:/tmp/x1", FileSystem.WriteMode.OVERWRITE);

    windowedStream.reduce(new ReduceFunction[SystemSessionCount]() {
      override def reduce(value1: SystemSessionCount, value2: SystemSessionCount): SystemSessionCount = {
         value1
      }
    })


    windowedStream.fold(new mutable.HashSet[String](), new FoldFunction[SystemSessionCount, mutable.HashSet[String]]() {
      override def fold(accumulator: mutable.HashSet[String], value: SystemSessionCount): mutable.HashSet[String] = {
        accumulator += value.sessionId
      }
    })




    // print the results with a single thread, rather than in parallel

    env.execute("Socket Window WordCount")
  }

  /** Data type for words with count */
  case class SystemSessionCount(system: String, sessionId: String, timestamp: Long, count: Int)

}
