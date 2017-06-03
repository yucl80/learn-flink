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

import org.apache.flink.api.common.functions.FoldFunction
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

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
object SocketWindowWordCount {

  /** Main program method */
  def main(args: Array[String]) : Unit = {

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

    env.enableCheckpointing(60000,CheckpointingMode.AT_LEAST_ONCE)
    // get input data by connecting to the socket
    val text = env.socketTextStream(hostname, port, '\n')

    // parse the data, group it, window it, and aggregate the counts 
    val  windowedStream= text
          .flatMap { w => w.split("\\s") }
          .map { w => WordWithCount("sys" ,w, 1) }
          .keyBy("system")
          .timeWindow(Time.seconds(60))

     val windowCounts= windowedStream .sum("count")

    // print the results with a single thread, rather than in parallel
    windowCounts.print().setParallelism(1)

    val foldData = windowedStream.fold(new mutable.HashSet[String](), new FoldFunction[WordWithCount, mutable.HashSet[String]]() {
      override def fold(accumulator: mutable.HashSet[String], value: WordWithCount): mutable.HashSet[String] = {
        accumulator += value.word
      }
    })






    //foldData.print()

    foldData.addSink(new SinkFunction[mutable.HashSet[String]] {
      override def invoke(value: mutable.HashSet[String]): Unit = {
          println(value)
      }
    })



    env.execute("Socket Window WordCount")
  }

  /** Data type for words with count */
  case class WordWithCount(system: String,word: String, count: Long)
}
