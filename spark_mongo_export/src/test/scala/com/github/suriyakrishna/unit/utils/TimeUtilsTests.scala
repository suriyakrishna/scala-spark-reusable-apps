package com.github.suriyakrishna.unit.utils

import com.github.suriyakrishna.unit.ApplicationTests
import com.github.suriyakrishna.utils.TimeUtils

class TimeUtilsTests extends ApplicationTests {

  override def setUp(): Unit = {
    // nothing to setup
  }

  test("getCurrentEpochTime method should pass") {
    TimeUtils.getCurrentEpochTime
  }

  test("getCurrentTime method should pass") {
    TimeUtils.getCurrentTime()
  }

  test("fromEpoch method using default format should match expected value") {
    val tms: Long = 1605369842202L
    assertResult("11/14/2020 21:34:02.202")(TimeUtils.fromEpoch(tms))
  }

  test("fromEpoch method using custom format should match expected value") {
    val tms: Long = 1605369842202L
    assertResult("11-14-20 21:34:02")(TimeUtils.fromEpoch(tms, "MM-dd-yy HH:mm:ss"))
  }

  test("toEpoch method using default format should match expected value") {
    val tms: String = "11/14/2020 21:34:02.202"
    assertResult(1605369842202L)(TimeUtils.toEpoch(tms))
  }

  test("toEpoch method using custom format should match expected value") {
    val tms: String = "11-14-20 21:34:02.202"
    assertResult(1605369842202L)(TimeUtils.toEpoch(tms, "MM-dd-yy HH:mm:ss.SSS"))
  }

  test("getTimeDiff method should throw Exception when startTime greater than endTime") {
    val startTime: Long = 1605369842202L
    val endTime: Long = 1605369842102L
    assertThrows[Exception] {
      TimeUtils.getTimeDiff(startTime, endTime)
    }
  }

  test("getTimeDiff method should match expected value") {
    val startTime: Long = 1605361842202L
    val endTime: Long = 1605369842202L
    assertResult("0days 2hours 13minutes 20seconds")(TimeUtils.getTimeDiff(startTime, endTime))
  }
}
