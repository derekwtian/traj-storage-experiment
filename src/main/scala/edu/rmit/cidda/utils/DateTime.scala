package edu.rmit.cidda.utils

import java.text.SimpleDateFormat
import java.util.{Date, TimeZone}

object DateTime {
  val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")
  sdf.setTimeZone(TimeZone.getTimeZone("GMT+0"))

  def timeStamp2Format(stamp: Long): String = {
    sdf.format(new Date(stamp))
  }

  def format2Stamp(datetime: String): Long = {
    sdf.parse(datetime).getTime
  }

  def main(args: Array[String]) {
    println(timeStamp2Format(1483574400000L))
    println(format2Stamp("1970-01-01T00:00:00"))
  }

}
