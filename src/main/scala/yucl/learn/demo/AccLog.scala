package yucl.learn.demo

import java.sql.Timestamp

import scala.collection.mutable

/**
  * Created by YuChunlei on 2017/5/27.
  */
case class AccLog(system: String, sessionid: String, clientip: String, status: Double, bytes: Double, time: Double, timestamp: Long, count: Int, uri: String)


case class UrlTime(var uri: String, var time: Double) extends Ordered[UrlTime] {
  override def toString: String = {
    uri + " :" + time
  }

  def compare(that: UrlTime) = {
    if (this.uri == that.uri)
      0
    else
      this.time.compareTo(that.time)
  }
}

case class Rst( urlSet: mutable.TreeSet[UrlTime],var count: Int, var bytes: Double, sessionSet: mutable.Set[String], ipSet: mutable.Set[String], count500: Int)

case class Result(system: String, count: Int, bytes: Double, sessionCount: Int, ipCount: Int, topUrl: mutable.Set[UrlTime], timestamp: Timestamp)
