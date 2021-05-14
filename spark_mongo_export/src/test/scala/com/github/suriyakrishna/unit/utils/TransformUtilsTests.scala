package com.github.suriyakrishna.unit.utils

import com.github.suriyakrishna.unit.ApplicationTests
import com.github.suriyakrishna.utils.TransformUtils
import org.apache.spark.sql.{AnalysisException, DataFrame, SparkSession}

class TransformUtilsTests extends ApplicationTests {

  private var spark: SparkSession = _
  private var df: DataFrame = _
  private var dataFilePath: String = _

  override def beforeAll(): Unit = {
    setUp()
  }

  override def setUp(): Unit = {
    spark = sharedSpark
    dataFilePath = ClassLoader.getSystemClassLoader.getResource("student_test.csv").getPath
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(dataFilePath)
  }

  test("transformSourceDF should throw SparkSQL Analysis Exception for Invalid SQL") {
    val sql: String = "TEST FAILURE"
    assertThrows[AnalysisException] {
      df.transform(TransformUtils.transformSourceDF(sql))
    }
  }

  test("transformSourceDF should return Transformed DataFrame") {
    val sql: String = "SELECT student_id AS _id, name, age, school, to_date(update_dt, 'yyyy-MM-dd') as update_dt, current_timestamp() AS record_insert_tms FROM source_view"
    val transformedDF = df.transform(TransformUtils.transformSourceDF(sql))

    assertResult(4)(transformedDF.count())
    assert(transformedDF.columns.toSet.intersect(Set("_id", "name", "age", "school", "update_dt", "record_insert_tms")).size == 6)
  }

}
