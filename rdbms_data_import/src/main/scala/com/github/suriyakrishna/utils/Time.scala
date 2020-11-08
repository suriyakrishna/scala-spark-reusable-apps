package com.github.suriyakrishna.utils

import java.text.SimpleDateFormat
import java.util.Date

object Time {

  private val defaultFormat: String = "MM/dd/yyyy HH:mm:ss.SSS"

  def getCurrentTimeStamp(format: String = defaultFormat): String = {
    return fromEpoch(getCurrentEpochTimeStamp, format)
  }

  def getCurrentEpochTimeStamp: Long = {
    return System.currentTimeMillis()
  }

  def fromEpoch(epoch: Long, format: String = defaultFormat): String = {
    val date: Date = new Date()
    date.setTime(epoch)
    val sdf = new SimpleDateFormat(format)
    return sdf.format(date)
  }

  def toEpoch(tms: String, format: String = defaultFormat): Long = {
    val sdf = new SimpleDateFormat(format)
    return sdf.parse(tms).getTime
  }

  def getTimeDiff(start: Long, end: Long): String = {
    if (start > end) throw new Exception("Start time is greater than end time. Start time should be less than end time")
    val diff: Long = end - start
    val diffSeconds = diff / 1000 % 60
    val diffMinutes = diff / (60 * 1000) % 60
    val diffHours = diff / (60 * 60 * 1000) % 24
    val diffDays = diff / (24 * 60 * 60 * 1000)
    return s"${diffDays}days ${diffHours}hours ${diffMinutes}minutes ${diffSeconds}seconds"
  }

}
