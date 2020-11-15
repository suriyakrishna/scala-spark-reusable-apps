package com.github.suriyakrishna.utils

import com.github.suriyakrishna.ApplicationTests

class TimeTests extends ApplicationTests {

  override def setUp(): Unit = {
    // Noting to setup
  }

  test("Test Current Epoch Timestamp method") {
    Time.getCurrentEpochTimeStamp
  }

  test("Test Current Timestamp method") {
    Time.getCurrentTimeStamp()
  }

  test("Test fromEpoch method using default format should match expected value") {
    val tms: Long = 1605369842202L
    assertResult("11/14/2020 21:34:02.202")(Time.fromEpoch(tms))
  }

  test("Test fromEpoch method using custom format should match expected value") {
    val tms: Long = 1605369842202L
    assertResult("11-14-20 21:34:02")(Time.fromEpoch(tms, "MM-dd-yy HH:mm:ss"))
  }

  test("Test toEpoch method using default format should match expected value") {
    val tms: String = "11/14/2020 21:34:02.202"
    assertResult(1605369842202L)(Time.toEpoch(tms))
  }

  test("Test toEpoch method using custom format should match expected value") {
    val tms: String = "11-14-20 21:34:02.202"
    assertResult(1605369842202L)(Time.toEpoch(tms, "MM-dd-yy HH:mm:ss.SSS"))
  }

  test("Test Time Difference method should throw Exception when startTime is greater than endTime") {
    val startTime: Long = 1605369842202L
    val endTime: Long = 1605369842102L
    assertThrows[Exception] {
      Time.getTimeDiff(startTime, endTime)
    }
  }

  test("Test Time Difference method should match expected value") {
    val startTime: Long = 1605361842202L
    val endTime: Long = 1605369842202L
    assertResult("0days 2hours 13minutes 20seconds")(Time.getTimeDiff(startTime, endTime))
  }
}
