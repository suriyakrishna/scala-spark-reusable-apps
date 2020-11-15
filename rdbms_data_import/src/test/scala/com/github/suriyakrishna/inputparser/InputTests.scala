package com.github.suriyakrishna.inputparser

import com.github.suriyakrishna.ApplicationTests

class InputTests extends ApplicationTests {

  private var input: Input = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    input = Input(
      "com.mysql.jdbc.Driver",
      "jdbc:mysql://ubuntu:3306",
      "hadoop",
      "hadoop12345",
      "world.city",
      "id",
      "/user/hadoop/mysql_import/world/city",
      true,
      false,
      "4",
      "parquet",
      "all",
      true,
      "ID",
      "id",
      "200",
      null,
      null,
      null
    )
  }

  override def setUp(): Unit = {
    // Nothing to setup
  }

  test("Test values of members of Input case class should match Expected values") {
    assertResult("com.mysql.jdbc.Driver")(input.driver)
    assertResult("jdbc:mysql://ubuntu:3306")(input.url)
    assertResult("hadoop")(input.username)
    assertResult("hadoop12345")(input.password)
    assertResult("world.city")(input.tableName)
    assertResult("id")(input.splitByColumn)
    assertResult("/user/hadoop/mysql_import/world/city")(input.targetDirectory)
    assertResult(true)(input.append)
    assertResult(false)(input.overwrite)
    assertResult("4")(input.numPartitions)
    assertResult("parquet")(input.format)
    assertResult("all")(input.columns)
    assertResult(true)(input.incremental)
    assertResult("ID")(input.incrementalType)
    assertResult("id")(input.incrementalColumn)
    assertResult("200")(input.incrementalColumnId)
    assertResult(null)(input.incrementalTimeFormat)
    assertResult(null)(input.incrementalStartTime)
    assertResult(null)(input.incrementalEndTime)
  }

  test("toString method should not contain username or password values"){
    val string: String = input.toString
    assert(!(string.contains("userName: hadoop") && string.contains("password: hadoop12345")))
  }
}
