package com.github.suriyakrishna.unit.utils

import com.github.suriyakrishna.unit.ApplicationTests
import com.github.suriyakrishna.utils.ReadUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

class ReadUtilsTests extends ApplicationTests with Logging {

  private var spark: SparkSession = _
  private var dataFilePath: String = _
  private var schemaPath: String = _

  override def beforeAll(): Unit = {
    setUp()
  }

  override def setUp(): Unit = {
    // Create SparkSession
    spark = sharedSpark
    schemaPath = ClassLoader.getSystemClassLoader.getResource("student-test-schema.json").getPath
    dataFilePath = ClassLoader.getSystemClassLoader.getResource("student_test.csv").getPath

  }

  test("JSONSchemaToStructSchema method with null value in filePath argument should throw RunTimeException") {
    assertThrows[RuntimeException] {
      ReadUtils.JSONSchemaToStructSchema(null)
    }
  }

  test("JSONSchemaToStructSchema method with empty string value in filePath argument should throw RunTimeException") {
    assertThrows[RuntimeException] {
      ReadUtils.JSONSchemaToStructSchema("")
    }
  }

  test("JSONSchemaToStructSchema method with schema file should return StructType") {
    val structSchema: StructType = ReadUtils.JSONSchemaToStructSchema(schemaPath)
    logInfo("Dataframe Schema\n" + structSchema.treeString)
    assert(structSchema.nonEmpty)
  }

  test("createSourceDF method should throw Runtime Exception when dataFileLocation argument is null") {
    assertThrows[RuntimeException] {
      ReadUtils.createSourceDF(dataLocation = null, format = "parquet", readOptions = null, schema = null)(spark)
    }
  }

  test("createSourceDF method should throw Runtime Exception when dataFileLocation argument is empty string") {
    assertThrows[RuntimeException] {
      ReadUtils.createSourceDF(dataLocation = "", format = "parquet", readOptions = null, schema = null)(spark)
    }
  }

  test("createSourceDF method without readOptions and schema should return DataFrame and validations should pass") {
    val df = ReadUtils.createSourceDF(dataLocation = dataFilePath, format = "csv", readOptions = null, schema = null)(spark)
    assert(df.columns.toSet.intersect(Set("_c0", "_c1", "_c2", "_c3", "_c4")).size == 5)
    assertResult(5)(df.count())
    assertResult(5)(df.columns.length)
  }

  test("createSourceDF method with readOptions and schema should return DataFrame and validations should pass") {
    val schema: StructType = ReadUtils.JSONSchemaToStructSchema(schemaPath)
    val readOptions: Map[String, String] = Map(
      "header" -> "true",
      "sep" -> "~",
      "inferSchema" -> "true"
    )
    val df = ReadUtils.createSourceDF(dataLocation = dataFilePath, format = "csv", readOptions = readOptions, schema = schema)(spark)
    assert(df.columns.toSet.intersect(schema.fields.map(_.name).toSet).size == 5)
    assertResult(4)(df.count())
    assertResult(5)(df.columns.length)
  }

  test("createSourceDF should throw Runtime Exception for unsupported file type") {
    assertThrows[RuntimeException] {
      ReadUtils.createSourceDF(dataLocation = dataFilePath, format = "avro", readOptions = null, schema = null)(spark)
    }
  }


}
