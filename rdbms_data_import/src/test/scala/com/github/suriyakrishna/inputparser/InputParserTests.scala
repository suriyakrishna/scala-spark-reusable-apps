package com.github.suriyakrishna.inputparser

import com.github.suriyakrishna.ApplicationTests

class InputParserTests extends ApplicationTests {

  private var args: Array[String] = _
  private var appName: String = this.getClass.getName.dropRight(1)

  override def setUp(): Unit = {
    // Nothing to configure
  }

  test("getUserInput should throw RuntimeException with message '--append and --overwrite both options cannot be used together'") {
    args = Array("--driver", "com.mysql.jdbc.Driver", "--url", "jdbc:mysql://ubuntu:3306", "--username", "hadoop", "--password", "hadoop", "--table", "world.city", "--split-by", "id", "--num-partitions", "1", "-target", "/user/hadoop/spark_data_import/world/city", "--format", "CSV", "--incremental", "--incremental-type", "ID", "--incremental-column", "id", "--incremental-id", "4080", "--append", "--overwrite")
    val command = InputParser.getCommandLine(args, appName)

    val error = intercept[RuntimeException] {
      InputParser.getUserInput(command)
    }

    assertResult("--append and --overwrite both options cannot be used together")(error.getMessage)
  }

  test("getUserInput should throw RuntimeException with message 'Output format cannot be AVRO. It can be only JSON/PARQUET/CSV.'") {
    args = Array("--driver", "com.mysql.jdbc.Driver", "--url", "jdbc:mysql://ubuntu:3306", "--username", "hadoop", "--password", "hadoop12345", "--table", "world.city", "--split-by", "id", "--num-partitions", "1", "-target", "/user/hadoop/spark_data_import/world/city", "--format", "avro", "--incremental", "--incremental-type", "ID", "--incremental-column", "id", "--incremental-id", "4080", "--append")
    val command = InputParser.getCommandLine(args, appName)

    val error = intercept[RuntimeException] {
      InputParser.getUserInput(command)
    }

    assertResult("Output format cannot be AVRO. It can be only JSON/PARQUET/CSV.")(error.getMessage)
  }

  test("getUserInput should throw RuntimeException with message '--overwrite option cannot be used with --incremental option'") {
    args = Array("--driver", "com.mysql.jdbc.Driver", "--url", "jdbc:mysql://ubuntu:3306", "--username", "hadoop", "--password", "hadoop12345", "--table", "world.city", "--split-by", "id", "--num-partitions", "1", "-target", "/user/hadoop/spark_data_import/world/city", "--incremental", "--incremental-type", "ID", "--incremental-column", "id", "--incremental-id", "4080", "--overwrite")
    val command = InputParser.getCommandLine(args, appName)

    val error = intercept[RuntimeException] {
      InputParser.getUserInput(command)
    }

    assertResult("--overwrite option cannot be used with --incremental option")(error.getMessage)
  }

  test("getUserInput should throw RuntimeException with message '--incremental option cannot be used without --append option'") {
    args = Array("--driver", "com.mysql.jdbc.Driver", "--url", "jdbc:mysql://ubuntu:3306", "--username", "hadoop", "--password", "hadoop12345", "--table", "world.city", "--split-by", "id", "--num-partitions", "1", "-target", "/user/hadoop/spark_data_import/world/city", "--incremental", "--incremental-type", "ID", "--incremental-column", "id", "--incremental-id", "4080")
    val command = InputParser.getCommandLine(args, appName)

    val error = intercept[RuntimeException] {
      InputParser.getUserInput(command)
    }

    assertResult("--incremental option cannot be used without --append option")(error.getMessage)
  }

  test("getUserInput should throw RuntimeException with message '--incremental-type option should be specified with --incremental option.'") {
    args = Array("--driver", "com.mysql.jdbc.Driver", "--url", "jdbc:mysql://ubuntu:3306", "--username", "hadoop", "--password", "hadoop12345", "--table", "world.city", "--split-by", "id", "--num-partitions", "1", "-target", "/user/hadoop/spark_data_import/world/city", "--incremental", "--incremental-column", "id", "--incremental-id", "4080", "--append")
    val command = InputParser.getCommandLine(args, appName)

    val error = intercept[RuntimeException] {
      InputParser.getUserInput(command)
    }

    assertResult("--incremental-type option should be specified with --incremental option.")(error.getMessage)
  }

  test("getUserInput should throw RuntimeException with message '--incremental-column option should be specified with --incremental option.'") {
    args = Array("--driver", "com.mysql.jdbc.Driver", "--url", "jdbc:mysql://ubuntu:3306", "--username", "hadoop", "--password", "hadoop12345", "--table", "world.city", "--split-by", "id", "--num-partitions", "1", "-target", "/user/hadoop/spark_data_import/world/city", "--incremental", "--incremental-type", "ID", "--incremental-id", "4080", "--append")
    val command = InputParser.getCommandLine(args, appName)

    val error = intercept[RuntimeException] {
      InputParser.getUserInput(command)
    }

    assertResult("--incremental-column option should be specified with --incremental option.")(error.getMessage)
  }

  test("getUserInput should throw RuntimeException with message '---incremental-id option should be specified for option --incremental-type 'ID''") {
    args = Array("--driver", "com.mysql.jdbc.Driver", "--url", "jdbc:mysql://ubuntu:3306", "--username", "hadoop", "--password", "hadoop12345", "--table", "world.city", "--split-by", "id", "--num-partitions", "1", "-target", "/user/hadoop/spark_data_import/world/city", "--incremental", "--incremental-type", "ID", "--incremental-column", "id", "--append")
    val command = InputParser.getCommandLine(args, appName)

    val error = intercept[RuntimeException] {
      InputParser.getUserInput(command)
    }

    assertResult("--incremental-id option should be specified for option --incremental-type 'ID'")(error.getMessage)
  }

  test("getUserInput should throw RuntimeException with message '--incremental-time-format, --incremental-start-time and --incremental-end-time options should be specified for option --incremental-type 'TIMESTAMP''") {
    args = Array("--driver", "com.mysql.jdbc.Driver", "--url", "jdbc:mysql://ubuntu:3306", "--username", "hadoop", "--password", "hadoop12345", "--table", "world.city", "--split-by", "id", "--num-partitions", "1", "-target", "/user/hadoop/spark_data_import/world/city", "--incremental", "--incremental-type", "TIMESTAMP", "--incremental-column", "last_update", "--append")
    val command = InputParser.getCommandLine(args, appName)

    val error = intercept[RuntimeException] {
      InputParser.getUserInput(command)
    }

    assertResult("--incremental-time-format, --incremental-start-time and --incremental-end-time options should be specified for option --incremental-type 'TIMESTAMP'")(error.getMessage)
  }

  test("getUserInput should throw RuntimeException with message '--incremental-id should not be used with --incremental-time-format, --incremental-start-time and --incremental-end-time options.") {
    args = Array("--driver", "com.mysql.jdbc.Driver", "--url", "jdbc:mysql://ubuntu:3306", "--username", "hadoop", "--password", "hadoop12345", "--table", "world.city", "--split-by", "id", "--num-partitions", "1", "-target", "/user/hadoop/spark_data_import/world/city", "--incremental", "--incremental-type", "TIMESTAMP", "--incremental-column", "last_update", "--append", "--incremental-id", "122", "--incremental-start-time", "1900-01-01 00:00:00", "--incremental-end-time", "2006-02-23 04:12:08", "--incremental-time-format", "yyyy-MM-dd HH:mm:ss")
    val command = InputParser.getCommandLine(args, appName)

    val error = intercept[RuntimeException] {
      InputParser.getUserInput(command)
    }

    assertResult("--incremental-id should not be used with --incremental-time-format, --incremental-start-time and --incremental-end-time options.")(error.getMessage)
  }

  test("getUserInput should throw RuntimeException with message 'Incremental import type cannot be TS. It can be only ID/TIMESTAMP.'") {
    args = Array("--driver", "com.mysql.jdbc.Driver", "--url", "jdbc:mysql://ubuntu:3306", "--username", "hadoop", "--password", "hadoop12345", "--table", "world.city", "--split-by", "id", "--num-partitions", "1", "-target", "/user/hadoop/spark_data_import/world/city", "--incremental", "--incremental-type", "ts", "--incremental-column", "last_update", "--append", "--incremental-start-time", "1900-01-01 00:00:00", "--incremental-end-time", "2006-02-23 04:12:08", "--incremental-time-format", "yyyy-MM-dd HH:mm:ss")
    val command = InputParser.getCommandLine(args, appName)

    val error = intercept[RuntimeException] {
      InputParser.getUserInput(command)
    }

    assertResult("Incremental import type cannot be TS. It can be only ID/TIMESTAMP.")(error.getMessage)
  }

  test("getUserInput should match expected values for History/Bulk Load") {
    args = Array("--driver", "com.mysql.jdbc.Driver", "--url", "jdbc:mysql://ubuntu:3306", "--username", "hadoop", "--password", "hadoop12345", "--table", "world.city", "--split-by", "id", "-target", "/user/hadoop/spark_data_import/world/city")

    val command = InputParser.getCommandLine(args, appName)

    val input: Input = InputParser.getUserInput(command)

    assertResult("com.mysql.jdbc.Driver")(input.driver)
    assertResult("jdbc:mysql://ubuntu:3306")(input.url)
    assertResult("hadoop")(input.username)
    assertResult("hadoop12345")(input.password)
    assertResult("world.city")(input.tableName)
    assertResult("id")(input.splitByColumn)
    assertResult("/user/hadoop/spark_data_import/world/city")(input.targetDirectory)
    assertResult(false)(input.append)
    assertResult(false)(input.overwrite)
    assertResult("4")(input.numPartitions)
    assertResult("parquet")(input.format)
    assertResult("all")(input.columns)
    assertResult(false)(input.incremental)
    assertResult(null)(input.incrementalType)
    assertResult(null)(input.incrementalColumn)
    assertResult(null)(input.incrementalColumnId)
    assertResult(null)(input.incrementalTimeFormat)
    assertResult(null)(input.incrementalStartTime)
    assertResult(null)(input.incrementalEndTime)
  }

  test("getUserInput should match expected values for Incremental Type ID") {
    args = Array("--driver", "com.mysql.jdbc.Driver", "--url", "jdbc:mysql://ubuntu:3306", "--username", "hadoop", "--password", "hadoop12345", "--table", "world.city", "--split-by", "id", "-target", "/user/hadoop/spark_data_import/world/city", "--num-partitions", "2", "--format", "json", "--incremental", "--incremental-type", "ID", "--incremental-column", "id", "--incremental-id", "200", "--append", "--columns", "id,name,code")

    val command = InputParser.getCommandLine(args, appName)

    val input: Input = InputParser.getUserInput(command)

    assertResult("com.mysql.jdbc.Driver")(input.driver)
    assertResult("jdbc:mysql://ubuntu:3306")(input.url)
    assertResult("hadoop")(input.username)
    assertResult("hadoop12345")(input.password)
    assertResult("world.city")(input.tableName)
    assertResult("id")(input.splitByColumn)
    assertResult("/user/hadoop/spark_data_import/world/city")(input.targetDirectory)
    assertResult(true)(input.append)
    assertResult(false)(input.overwrite)
    assertResult("2")(input.numPartitions)
    assertResult("json")(input.format)
    assertResult("id,name,code")(input.columns)
    assertResult(true)(input.incremental)
    assertResult("id")(input.incrementalType)
    assertResult("id")(input.incrementalColumn)
    assertResult("200")(input.incrementalColumnId)
    assertResult(null)(input.incrementalTimeFormat)
    assertResult(null)(input.incrementalStartTime)
    assertResult(null)(input.incrementalEndTime)
  }

  test("getUserInput should match expected values for Incremental Type TIMESTAMP") {
    args = Array("--driver", "com.mysql.jdbc.Driver", "--url", "jdbc:mysql://ubuntu:3306", "--username", "hadoop", "--password", "hadoop12345", "--table", "world.city", "--split-by", "id", "-target", "/user/hadoop/spark_data_import/world/city", "--num-partitions", "2", "--format", "csv", "--incremental", "--incremental-type", "TIMESTAMP", "--incremental-column", "last_update", "--incremental-time-format", "yyyy-MM-dd HH:mm:ss", "--incremental-start-time", "1900-01-01 00:00:00", "--incremental-end-time", "2006-02-15 22:19:54", "--append")

    val command = InputParser.getCommandLine(args, appName)

    val input: Input = InputParser.getUserInput(command)

    assertResult("com.mysql.jdbc.Driver")(input.driver)
    assertResult("jdbc:mysql://ubuntu:3306")(input.url)
    assertResult("hadoop")(input.username)
    assertResult("hadoop12345")(input.password)
    assertResult("world.city")(input.tableName)
    assertResult("id")(input.splitByColumn)
    assertResult("/user/hadoop/spark_data_import/world/city")(input.targetDirectory)
    assertResult(true)(input.append)
    assertResult(false)(input.overwrite)
    assertResult("2")(input.numPartitions)
    assertResult("csv")(input.format)
    assertResult("all")(input.columns)
    assertResult(true)(input.incremental)
    assertResult("timestamp")(input.incrementalType)
    assertResult("last_update")(input.incrementalColumn)
    assertResult(null)(input.incrementalColumnId)
    assertResult("yyyy-MM-dd HH:mm:ss")(input.incrementalTimeFormat)
    assertResult("1900-01-01 00:00:00")(input.incrementalStartTime)
    assertResult("2006-02-15 22:19:54")(input.incrementalEndTime)
  }
}