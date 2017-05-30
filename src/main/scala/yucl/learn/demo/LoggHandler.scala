package yucl.learn.demo

import java.text.SimpleDateFormat
import java.util.Properties

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.util.Collector
import yucl.learn.demo.LoggHandler.UrlTime

import scala.collection.immutable.HashMap.HashTrieMap
import scala.collection.mutable
import scala.util.parsing.json.JSON

/**
  * Created by YuChunlei on 2017/5/27.
  */
object LoggHandler {
  def main(args: Array[String]) {
    val properties = new Properties
    properties.setProperty("bootstrap.servers", "192.168.21.27:9092")
    properties.setProperty("zookeeper.connect", "192.168.21.12:2181,192.168.21.13:2181,192.168.21.14:2181")
    properties.setProperty("group.id", "key-words-alert")
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
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
            j.getOrElse("message", 0d).asInstanceOf[String]
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
      val maxOutOfOrderness = 5 // 3.5 seconds
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

    case class Result(system: String, var count: Int, var bytes: Double, var sessionCount: Int, var ipCount: Int, var topUrl: mutable.TreeSet[UrlTime])
    val windowedData = timedData.keyBy(0)
      .window(TumblingEventTimeWindows.of(Time.seconds(300)))

    val resultData = windowedData.apply(new WindowFunction[AccLog, Result, Tuple, TimeWindow] {
      override def apply(key: Tuple, window: TimeWindow, input: Iterable[AccLog], out: Collector[Result]): Unit = {
        val map = new mutable.HashMap[String, Result]()
        val sessionMap = new mutable.HashMap[String, mutable.HashSet[String]]()
        val ipMap = new mutable.HashMap[String, mutable.HashSet[String]]()
        val topUrlMap = new mutable.HashMap[String, mutable.TreeSet[UrlTime]]
        val topN = 10
        input.foreach(x => {
          val r = map.getOrElse(x.system, new Result(x.system, 0, 0d, 0, 0, null))
          r.count += 1
          r.bytes += x.bytes
          map.put(x.system, r)

          val sessionSet = sessionMap.getOrElse(x.system, new mutable.HashSet[String]())
          sessionSet += x.sessionid
          sessionMap.put(x.system, sessionSet)

          val ipSet = ipMap.getOrElse(x.system, new mutable.HashSet[String]())
          ipSet += x.clientip
          ipMap.put(x.system, ipSet)

          val urlSet = topUrlMap.getOrElse(x.system, new mutable.TreeSet[UrlTime])
          val v = urlSet.filter(u => u.url == x.uri && u.time < x.time).last
          if (v != null) {
            urlSet -= v
            urlSet += new UrlTime(x.uri, x.time)
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
        })
        map.foreach(x => {
          x._2.sessionCount = sessionMap.get(x._1).get.size
          x._2.ipCount = ipMap.get(x._1).get.size
          x._2.topUrl = topUrlMap.get(x._1).get
          out.collect(x._2)
        })
      }
    })

    resultData.print

    /*    val count = timedData.keyBy(0)
          .window(TumblingEventTimeWindows.of(Time.seconds(300)))
          .sum("count").map(x => new Tuple3[String, String, Int](x.system, "count", x.count))
        val maxTime = timedData.keyBy(0)
          .window(TumblingEventTimeWindows.of(Time.seconds(300)))
          .max("time").map(x => new Tuple3[String, String, Double](x.system, "maxtime", x.time))
        val sumBytes = timedData.keyBy(0)
          .window(TumblingEventTimeWindows.of(Time.seconds(300)))
          .sum("bytes").map(x => new Tuple3[String, String, Double](x.system, "sumBytes", x.bytes))
        var joinedStreams = count.join(maxTime).where(x => x._1).equalTo(x => x._1)
          .window(TumblingEventTimeWindows.of(Time.seconds(300)))
          .apply(new JoinFunction[(String, String, Int), (String, String, Double), (String, Int, Double)] {
            override def join(first: (String, String, Int), second: (String, String, Double)): (String, Int, Double) = {
              (first._1, first._3, second._3)
            }
          }).print()

        sumBytes.addSink(new SinkFunction[(String, String, Double)]() {
          override def invoke(value: (String, String, Double)): Unit = {

          }
        })*/



    try {
      env.setParallelism(2)
      env.execute("key words alert")
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }

}


class UrlTime(var url: String, var time: Double) extends Ordered[UrlTime] {
  override def toString: String = {
    url + " :" + time
  }

  def compare(that: UrlTime) = {
    if (this.url == that.url)
      0
    else
      this.time.compareTo(that.time)
  }
}
