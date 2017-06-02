package yucl.learn.demo

import java.text.SimpleDateFormat
import java.util.Properties

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.util.Collector

import scala.collection.immutable.HashMap.HashTrieMap
import scala.util.parsing.json.JSON

/**
  * Created by YuChunlei on 2017/5/25.
  */
object AccessLogHandler {
  def main(args: Array[String]) {
    val properties = new Properties
    properties.setProperty("bootstrap.servers", "192.168.21.27:9092")
    // only required for Kafka 0.8
    properties.setProperty("zookeeper.connect", "192.168.21.12:2181,192.168.21.13:2181,192.168.21.14:2181")
    properties.setProperty("group.id", "key-words-alert")
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    /* val stream  = env.addSource(new FlinkKafkaConsumer08[ObjectNode ]("parsed-acclog", new JSONKeyValueDeserializationSchema(false), properties))
    stream.print()*/

    /**
      * 1> {"message":"[26/May/2017:06:31:08 +0800] 8A5B223609BE456156C8FA66B9524660 192.168.8.152 - - GET 200 - 1 \"http-20039-50\" \"-\" \"-\" \"/SL_LES/proxyKeepAlive\" \"\" \"-\" \"Jetty/9.2.11.v20150529\"","@timestamp":"2017-05-26T06:31:08.000+08:00","host":"szebpint1","path":"/mwbase/applogs/rtlog/LES_0_INT_2_1_1/localhost.2017-05-26.acc","sessionid":"8A5B223609BE456156C8FA66B9524660","clientip":"192.168.8.152","xforwardedfor":"-","auth":"-","verb":"GET","response":200,"time":1,"thread":"http-20039-50","uidcookie":"-","adcookie":"-","uri":"/SL_LES/proxyKeepAlive","referrer":"-","agent":"Jetty/9.2.11.v20150529","system":"LES","instance":"LES_0_INT_2_1_1","agentinfo":{"name":"Other","os":"Other","os_name":"Other","device":"Other"}}
      * 1> {"message":"[26/May/2017:06:31:13 +0800] D93E6C19894D152266329AC52317A9E8 192.168.8.152 - - GET 200 - 1 \"http-20039-50\" \"-\" \"-\" \"/SL_LES/proxyKeepAlive\" \"\" \"-\" \"Jetty/9.2.11.v20150529\"","@timestamp":"2017-05-26T06:31:13.000+08:00","host":"szebpint1","path":"/mwbase/applogs/rtlog/LES_0_INT_2_1_1/localhost.2017-05-26.acc","sessionid":"D93E6C19894D152266329AC52317A9E8","clientip":"192.168.8.152","xforwardedfor":"-","auth":"-","verb":"GET","response":200,"time":1,"thread":"http-20039-50","uidcookie":"-","adcookie":"-","uri":"/SL_LES/proxyKeepAlive","referrer":"-","agent":"Jetty/9.2.11.v20150529","system":"LES","instance":"LES_0_INT_2_1_1","agentinfo":{"name":"Other","os":"Other","os_name":"Other","device":"Other"}}
      * 1> {"message":"[26/May/2017:06:31:09 +0800] 6FCCB889076176370FF9914F12E4F7A2 192.168.8.152 - - GET 200 - 1 \"http-20039-27\" \"-\" \"-\" \"/SL_LES/proxyCheckHealth\" \"\" \"-\" \"Jetty/9.2.11.v20150529\"","@timestamp":"2017-05-26T06:31:09.000+08:00","host":"szebpint1","path":"/mwbase/applogs/rtlog/LES_0_INT_2_1_1/localhost.2017-05-26.acc","sessionid":"6FCCB889076176370FF9914F12E4F7A2","clientip":"192.168.8.152","xforwardedfor":"-","auth":"-","verb":"GET","response":200,"time":1,"thread":"http-20039-27","uidcookie":"-","adcookie":"-","uri":"/SL_LES/proxyCheckHealth","referrer":"-","agent":"Jetty/9.2.11.v20150529","system":"LES","instance":"LES_0_INT_2_1_1","agentinfo":{"name":"Other","os":"Other","os_name":"Other","device":"Other"}}
      */
    val stream: DataStream[String] = env.addSource(new FlinkKafkaConsumer08[String]("parsed-acclog", new SimpleStringSchema, properties))

    /* val data =  stream.map(new MapFunction[String, (String, String, String, Double, Double, Double)] {
       override def map(value: String): (String, String, String, Double, Double, Double) = {
         try {
           val json: Option[Any] = JSON.parseFull(value)
           val j = json.get.asInstanceOf[HashTrieMap[String, Any]]
           new Tuple6(
             j.getOrElse("system", "").asInstanceOf[String],
             j.getOrElse("sessionid", "").asInstanceOf[String],
             j.getOrElse("clientip", "").asInstanceOf[String],
             j.getOrElse("response", 0d).asInstanceOf[Double],
             j.getOrElse("bytes", 0d).asInstanceOf[Double],
             j.getOrElse("time", 0d).asInstanceOf[Double]
           )
         } catch {
           case e: Exception => {
             println("parse failed: " + value)
             e.printStackTrace()
           }
             null
         }
       }

     }).filter(a => a!= null).filter( a => a. _4 != 200)

     val tableEnv = TableEnvironment.getTableEnvironment(env)
     tableEnv.registerDataStream("acclog",data)
     val tableQuery = tableEnv.sql("SELECT *   FROM acclog ")
     tableEnv.toDataStream[(String, String, String, Double, Double, Double)](tableQuery).print()*/


    /* val tableEnv = TableEnvironment.getTableEnvironment(env)
     tableEnv.registerDataStream("acclog",data)
     val tableQuery = tableEnv.sql("SELECT * FROM acclog ")
     tableEnv.toDataStream(tableQuery)*/


    // run a SQL query on the Table and retrieve the result as a new Table
    /*  val result = tableEnv.sql(
        "SELECT STREAM * FROM Products WHERE name LIKE '%Apple%'")

       data.keyBy(0)
        .timeWindow(Time.seconds(5))
        .max(5)
        .print()*/


    val data = stream.map(new MapFunction[String, AccLog] {
      override def map(value: String): AccLog = {
        try {
          /* val gson = new Gson
           val mapType = new TypeToken[java.util.TreeMap[String, String]] {}.getType
           val map = gson.fromJson[java.util.TreeMap[String, String]](value, mapType).asScala*/
          val json: Option[Any] = JSON.parseFull(value)
          val j = json.get.asInstanceOf[HashTrieMap[String, Any]]
          var t = j.getOrElse("@timestamp", 0d).asInstanceOf[String]
          val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-mm-ddTHH:MI:ss.sss+Z")
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

    val withTimestampsAndWatermarks = data.assignAscendingTimestamps(_.timestamp)

    val withTimestampsAndWatermarks2 = data.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[AccLog](Time.seconds(10)) {
      override def extractTimestamp(element: AccLog): Long = {
        element.timestamp
      }
    })

    /**
      * This generator generates watermarks assuming that elements come out of order to a certain degree only.
      * The latest elements for a certain timestamp t will arrive at most n milliseconds after the earliest
      * elements for timestamp t.
      */
    data.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[AccLog] {
      val maxOutOfOrderness = 3500 // 3.5 seconds

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

    /**
      * This generator generates watermarks that are lagging behind processing time by a certain amount.
      * It assumes that elements arrive in Flink after at most a certain time.
      */
    data.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[AccLog] {
       val maxTimeLag = 5000

      override def extractTimestamp(element: AccLog, previousElementTimestamp: Long): Long = {
         element.timestamp
      }

      override def getCurrentWatermark: Watermark = {
        // return the watermark as current time minus the maximum time lag
        return new Watermark(System.currentTimeMillis() - maxTimeLag);
      }
    })

    data.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[AccLog](Time.seconds((60))) {
      override def extractTimestamp(element: AccLog): Long = {
         element.timestamp
      }
    })

    data.keyBy("system").window(TumblingEventTimeWindows.of(Time.seconds(5))).sum("count").print()

    data.keyBy("system").timeWindow(Time.milliseconds(1000))
      .apply(new WindowFunction[AccLog,AccLog,Tuple,TimeWindow] {
        override def apply(key: Tuple, window: TimeWindow, input: Iterable[AccLog], out: Collector[AccLog]): Unit = {
           var  max = 0d
            input.foreach(x => {
              if ( x.time > max) max = x.time
            })

        }
      })

    /*val tableEnv = TableEnvironment.getTableEnvironment(env)
    tableEnv.registerDataStream("acclog",data)
    val tableQuery = tableEnv.sql("SELECT * FROM acclog ").select("sessionid")
    tableEnv.toDataStream[String](tableQuery).print()*/


    // val stream: DataStream[] = env.addSource(new FlinkKafkaConsumer08[SimpleStringSchema ]("parsed-acclog", new KeyedDeserializationSchemaWrapper[SimpleStringSchema](), properties))

    //stream.filter(new KeyWordsFilterFunction()).writeAsText("/tmp/", FileSystem.WriteMode.OVERWRITE);
    /*  stream.filter(new FilterFunction[ObjectNode] {
        override def filter(value: ObjectNode): Boolean = {
          if (value.get("uri").toString.contains("SL_GOA")) true

          false
        }
      }).print()*/

    // stream.writeAsText("/tmp/", FileSystem.WriteMode.OVERWRITE);
    /* stream.filter(new FilterFunction[String] {
       override def filter(value: String): Boolean = {
         if (value.indexOf("200") == -1) true

         false
       }
     }).print()*/

    try {
      env.setParallelism(2)
      env.execute("key words alert")
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }
}

