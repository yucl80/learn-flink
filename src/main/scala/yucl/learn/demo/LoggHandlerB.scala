package yucl.learn.demo

import java.text.SimpleDateFormat
import java.util
import java.util.Properties

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.util.Collector

import scala.collection.immutable.HashMap.HashTrieMap
import scala.collection.mutable
import scala.util.parsing.json.JSON
/**
  * Created by YuChunlei on 2017/5/27.
  */
object LoggHandlerB {
  case class DA( urlSet : util.TreeSet[UrlTime])

  def main(args: Array[String]) {
    val properties = new Properties
    properties.setProperty("bootstrap.servers", "192.168.21.27:9092")
    properties.setProperty("zookeeper.connect", "192.168.21.12:2181,192.168.21.13:2181,192.168.21.14:2181")
    properties.setProperty("group.id", "key-words-alert")
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.enableCheckpointing(10000, CheckpointingMode.AT_LEAST_ONCE)

    val stream: DataStream[String] = env.addSource(new FlinkKafkaConsumer08[String]("parsed-acclog", new SimpleStringSchema, properties))
    val data = stream.map(new MapFunction[String, AccLog] {
      override def map(value: String): AccLog = {
        try {
          val json: Option[Any] = JSON.parseFull(value)
          val j = json.get.asInstanceOf[HashTrieMap[String, Any]]
          var t = j.getOrElse("@timestamp", 0d).asInstanceOf[String]
          val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
          val timestamp = sdf.parse(t).getTime
          new AccLog(
            j.getOrElse("system", "").asInstanceOf[String],
            j.getOrElse("sessionid", "").asInstanceOf[String],
            j.getOrElse("clientip", "").asInstanceOf[String],
            j.getOrElse("response", 0d).asInstanceOf[Double],
            j.getOrElse("bytes", 0d).asInstanceOf[Double],
            j.getOrElse("time", 0d).asInstanceOf[Double],
            timestamp,
            1,
            j.getOrElse("uri", "").asInstanceOf[String]
          )
        } catch {
          case e: Exception => {
            println("parse failed: " + value)
            e.printStackTrace()
          }
            null
        }
      }
    }).filter(x => x != null)
    val timedData = data.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[AccLog] {
      val maxOutOfOrderness = 5000 // 3.5 seconds
      var currentMaxTimestamp = 0L

      override def extractTimestamp(element: AccLog, previousElementTimestamp: Long): Long = {
        val timestamp: Long = element.timestamp
        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp)
        timestamp
      }

      override def getCurrentWatermark: Watermark = {
        return new Watermark(currentMaxTimestamp - maxOutOfOrderness)
      }
    })
    val keyedStream = timedData.keyBy(0)
    val windowedData = keyedStream
      .window(TumblingEventTimeWindows.of(Time.minutes(2)))



    val result1 = windowedData.fold(new mutable.TreeSet[UrlTime](),
      (s: mutable.TreeSet[UrlTime], x: AccLog) => {
        aggregate(s, x)
        s
      },
      (key: Tuple, window: TimeWindow, input: Iterable[mutable.TreeSet[UrlTime]], out: Collector[Result]) => {
        input.foreach(e => {
          try {

             // out.collect(new Result(key.getField(0), 0,0,0,0, null, new Timestamp(window.getEnd)))
          } catch {
            case e: Exception => e.printStackTrace()
          }
        })
      }
    ).print()

    try {
      env.setParallelism(2)
      env.execute("test")
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }

  def aggregate(urlSet: mutable.TreeSet[UrlTime], x: AccLog): Unit = {
  /*  s.count += 1
    s.bytes += x.bytes
    s.sessionSet += x.sessionid
    s.ipSet += x.clientip*/
    val topN = 10
    if (urlSet != null) {
      if (urlSet != null) {
        val vs = urlSet.filter(u => u.uri == x.uri)
        if (vs.size > 0) {
          val f = vs.last
          if (f.time < x.time) {
            urlSet -= vs.last
            urlSet += new UrlTime(x.uri, x.time)
          }
        } else {
          if (urlSet.size <= topN) {
            urlSet += new UrlTime(x.uri, x.time)
          } else {
            val last = urlSet.last
            if (x.time > last.time) {
              urlSet += new UrlTime(x.uri, x.time)
              urlSet -= urlSet.last
            }
          }
        }
      }
    }


  }

}



