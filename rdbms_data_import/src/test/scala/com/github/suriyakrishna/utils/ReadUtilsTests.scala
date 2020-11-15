package com.github.suriyakrishna.utils

import com.github.suriyakrishna.ApplicationTests
import com.github.suriyakrishna.inputparser.Input
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}


class ReadUtilsTests extends ApplicationTests with Logging {

  private var spark: SparkSession = _
  private var df: DataFrame = _
  private var dataFilePath: String = ClassLoader.getSystemClassLoader.getResource("testData/world.city.csv").getPath
  private var input: Input = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder().master("local").appName("Test").getOrCreate()
    setUp()
  }

  override def setUp(): Unit = {
    logInfo("Test Data File: " + dataFilePath)
    df = spark.read.option("inferSchema", "true").option("header", "true").csv(dataFilePath)
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

  test("getTimestampIncrementalCondition should generate the expected condition and df count after filter should be 3") {
    val condition = ReadUtils.getTimestampIncrementalCondition("last_update", "yyyy-MM-dd HH:mm:ss", "2006-02-15 22:19:54", "2010-02-15 22:19:54")
    assertResult("((to_timestamp(`last_update`, 'yyyy-MM-dd HH:mm:ss') >= to_timestamp('2006-02-15 22:19:54', 'yyyy-MM-dd HH:mm:ss')) AND (to_timestamp(`last_update`, 'yyyy-MM-dd HH:mm:ss') < to_timestamp('2010-02-15 22:19:54', 'yyyy-MM-dd HH:mm:ss')))")(condition.toString())
    val count = df.filter(condition).count()
    assertResult(3)(count)
  }

  test("getIncrementalCondition should generate the expected condition and df count after filter should be 2") {
    val condition = ReadUtils.getIncrementalCondition(df, "id", "3")
    assertResult("(id > CAST(3 AS INT))")(condition.toString())
    val count = df.filter(condition).count()
    assertResult(2)(count)
  }

  test("getBoundary return BoundaryValue and should match expected value") {
    val bound: BoundValues = ReadUtils.getBoundary(df, "world.city", "id")
    assertResult("BoundValues(lower=1, upper=5)")(bound.toString)
  }

  test("getBoundary BoundaryValue null should throw RuntimeException") {
    //    val error = intercept[RuntimeException]{
    //      ReadUtils.getBoundary(df.filter(col("id") > 5), "world.city", "id")
    //    }
    //    assertResult("Boundary values are null. Check data in table and rerun.")(error.getMessage)
  }

  test("getBoundaryQueryJDBCOptions should match expected values") {
    val options: Map[String, String] = ReadUtils.getBoundaryQueryJDBCOptions(input)
    assertResult("jdbc:mysql://ubuntu:3306")(options("url"))
    assertResult("com.mysql.jdbc.Driver")(options("driver"))
    assertResult("world.city")(options("dbtable"))
    assertResult("hadoop")(options("user"))
    assertResult("hadoop12345")(options("password"))
  }

  test("getSparkReadJBDCOptions should match expected values") {
    val options: Map[String, String] = ReadUtils.getSparkReadJBDCOptions(input, BoundValues("1", "5"))
    assertResult("jdbc:mysql://ubuntu:3306")(options("url"))
    assertResult("com.mysql.jdbc.Driver")(options("driver"))
    assertResult("world.city")(options("dbtable"))
    assertResult("hadoop")(options("user"))
    assertResult("hadoop12345")(options("password"))
    assertResult("id")(options("partitionColumn"))
    assertResult("1")(options("lowerBound"))
    assertResult("5")(options("upperBound"))
    assertResult("4")(options("numPartitions"))
  }
}
