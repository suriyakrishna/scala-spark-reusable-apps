package com.github.suriyakrishna.unit.utils

import com.github.suriyakrishna.unit.ApplicationTests
import com.github.suriyakrishna.utils.WriteUtils
import com.mongodb.MongoException
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row, SaveMode}
import org.mockito.scalatest.MockitoSugar

class WriteUtilsTests extends ApplicationTests with MockitoSugar {

  private var writeOptions: Map[String, String] = _

  override def beforeAll(): Unit = {
    setUp()
  }

  override def setUp(): Unit = {
    writeOptions = Map(
      "database" -> "world",
      "collection" -> "city"
    )
  }

  test("writeToMongo method should throw Runtime Exception for unsupported writeMode") {
    val df = mock[DataFrame]

    assertThrows[RuntimeException] {
      WriteUtils.writeToMongo(dataFrame = df, writeMode = "test", numPartitions = 10, options = writeOptions)
    }
  }

  test("writeToMongo method should invoke DataFrame save method") {

    // define mocks
    val df = mock[DataFrame]
    val dataFrameWriter = mock[DataFrameWriter[Row]]

    val partitions: Int = 10
    val saveMode: SaveMode = SaveMode.Overwrite
    val format: String = "com.mongodb.spark.sql"

    // when
    doReturn(df).when(df).repartition(partitions)
    doReturn(dataFrameWriter).when(df).write
    doReturn(dataFrameWriter).when(dataFrameWriter).mode(saveMode)
    doReturn(dataFrameWriter).when(dataFrameWriter).format(format)
    doReturn(dataFrameWriter).when(dataFrameWriter).options(writeOptions)
    doNothing.when(dataFrameWriter).save()

    // call
    WriteUtils.writeToMongo(dataFrame = df, writeMode = "overwrite", numPartitions = partitions, options = writeOptions)

    // verify
    verify(df).repartition(partitions)
    verify(df).write
    verify(dataFrameWriter).mode(saveMode)
    verify(dataFrameWriter).format(format)
    verify(dataFrameWriter).options(writeOptions)
    verify(dataFrameWriter).save()
  }

  test("writeToMongo method should throw Runtime Exception in case if DataFrameWriter Encounters an Exception") {

    // define mocks
    val df = mock[DataFrame]
    val dataFrameWriter = mock[DataFrameWriter[Row]]

    val partitions: Int = 10
    val saveMode: SaveMode = SaveMode.Overwrite
    val format: String = "com.mongodb.spark.sql"

    // when
    doReturn(df).when(df).repartition(partitions)
    doReturn(dataFrameWriter).when(df).write
    doReturn(dataFrameWriter).when(dataFrameWriter).mode(saveMode)
    doReturn(dataFrameWriter).when(dataFrameWriter).format(format)
    doReturn(dataFrameWriter).when(dataFrameWriter).options(writeOptions)
    doThrow(new MongoException("Testing Exception writeToMongo")).when(dataFrameWriter).save()

    // call
    assertThrows[RuntimeException] {
      WriteUtils.writeToMongo(dataFrame = df, writeMode = "overwrite", numPartitions = partitions, options = writeOptions)
    }

    // verify
    verify(df).repartition(partitions)
    verify(df).write
    verify(dataFrameWriter).mode(saveMode)
    verify(dataFrameWriter).format(format)
    verify(dataFrameWriter).options(writeOptions)
    verify(dataFrameWriter).save()
  }

}
