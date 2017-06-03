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

import java.sql.Timestamp

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala._
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
object SocketWindowWordTest {

  /** Main program method */
  def main(args: Array[String]): Unit = {

    // the host and the port to connect to
    var hostname: String = "192.168.142.145"
    var port: Int = 9999

    /*try {
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

    env.enableCheckpointing(60000, CheckpointingMode.AT_LEAST_ONCE)
    // get input data by connecting to the socket
    val text = env.socketTextStream(hostname, port, '\n')

    // parse the data, group it, window it, and aggregate the counts 
    val stream = text
      .map { w => {
        val ss = w.split("\\s")
        WordWithCount(ss(0), ss(1), 1)
      } }

    val windowedStream = stream
      .keyBy("system")
      .timeWindow(Time.seconds(60))

   // stream.keyBy("system","word").timeWindow(Time.seconds(60)).sum("count").print()

    /* val windowCounts= windowedStream .sum("count")

    // print the results with a single thread, rather than in parallel
    windowCounts.print().setParallelism(1)

    val foldData = windowedStream.fold(new mutable.HashSet[String](), new FoldFunction[WordWithCount, mutable.HashSet[String]]() {
      override def fold(accumulator: mutable.HashSet[String], value: WordWithCount): mutable.HashSet[String] = {
        accumulator += value.word
      }
    })*/

   /* val result = windowedStream.fold(new mutable.HashSet[String](),
      (s: mutable.HashSet[String], a: WordWithCount) => {
        s += a.word
      },
      (key: Tuple, window: TimeWindow, input: Iterable[mutable.HashSet[String]], out: Collector[(String, mutable.HashSet[String])]) => {
        var result = new mutable.HashMap[String, String]()
        input.foreach(e => {
          out.collect((key.getField[String](0), e))
          println(new Timestamp(window.getStart), new Timestamp(window.getEnd))
        })

      }
    )*/
    case class DT(ws:mutable.HashSet[String],var obj: Object)
    val result = windowedStream.fold(new  DT(new mutable.HashSet[String](),null),
      (s:DT, a: WordWithCount) => {
        s.ws += a.word
        if(s.obj == null){
          s.obj= new mutable.TreeSet[String]()
        }
        val ts :mutable.TreeSet[String] = s.obj.asInstanceOf[mutable.TreeSet[String]]
        ts += a.word
        s
      },
      (key: Tuple, window: TimeWindow, input: Iterable[DT], out: Collector[DT]) => {
        input.foreach(e => {
          out.collect(e)
          println(new Timestamp(window.getStart), new Timestamp(window.getEnd))
        })

      }
    ).print()
    /*val result = windowedStream.fold(new  Tuple2[mutable.HashSet[String],mutable.ListBuffer[mutable.TreeSet[String]]](new mutable.HashSet[String](),new mutable.ListBuffer[mutable.TreeSet[String]]()),
      (s: (mutable.HashSet[String],mutable.ListBuffer[mutable.TreeSet[String]]), a: WordWithCount) => {
         s._1 += a.word
         if(s._2.size == 0){

           s._2 append( new mutable.TreeSet[String]())
         }

         s._2.last += a.word
        s
      },
      (key: Tuple, window: TimeWindow, input: Iterable[Tuple2[mutable.HashSet[String],mutable.ListBuffer[mutable.TreeSet[String]]]], out: Collector[(String, mutable.HashSet[String],mutable.ListBuffer[mutable.TreeSet[String]])]) => {
        input.foreach(e => {
          out.collect((key.getField[String](0), e._1,e._2))
          println(new Timestamp(window.getStart), new Timestamp(window.getEnd))
        })

      }
    ).print()*/






    //foldData.print()

   /* result.addSink(new SinkFunction[(String, mutable.HashSet[String])] {
      override def invoke(value: (String, mutable.HashSet[String])): Unit = {
        println(value._1, value._2.size)
      }
    })*/

    env.execute("Socket Window WordCount")
  }

  /** Data type for words with count */
  case class WordWithCount(system: String, word: String, count: Long)

}
