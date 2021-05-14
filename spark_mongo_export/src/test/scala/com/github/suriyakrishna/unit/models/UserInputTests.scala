package com.github.suriyakrishna.unit.models

import com.github.suriyakrishna.models.UserInput
import com.github.suriyakrishna.unit.ApplicationTests

class UserInputTests extends ApplicationTests {

  private var userInput: UserInput = null

  override def beforeAll(): Unit = {
    setUp()
  }

  override def setUp(): Unit = {
    userInput = UserInput(
      mongoURI = "mongodb://localhost:27017",
      dbName = "world",
      collectionName = "city",
      dataLocation = "/user/hadoop/spark_jdbc_import_out/world/city",
      fileFormat = "parquet",
      writeMode = "overwrite",
      schemaPath = "./world_city_schema.json",
      transformationSQL = "SELECT * FROM source_view",
      readOptions = null,
      writeOptions = Map("database" -> "world", "collection" -> "city", "replaceDocument" -> "false", "forceInsert" -> "false"),
      numPartitions = 10
    )
  }

  test("UserInput members value should match with expected value") {
    assertResult("mongodb://localhost:27017")(userInput.mongoURI)
    assertResult("world")(userInput.dbName)
    assertResult("city")(userInput.collectionName)
    assertResult("/user/hadoop/spark_jdbc_import_out/world/city")(userInput.dataLocation)
    assertResult("parquet")(userInput.fileFormat)
    assertResult("overwrite")(userInput.writeMode)
    assertResult("./world_city_schema.json")(userInput.schemaPath)
    assertResult("SELECT * FROM source_view")(userInput.transformationSQL)
    assertResult(null)(userInput.readOptions)
    assertResult(Map("database" -> "world", "collection" -> "city", "replaceDocument" -> "false", "forceInsert" -> "false"))(userInput.writeOptions)
    assertResult(10)(userInput.numPartitions)
  }

  test("toString method should not return string with mongoUri") {
    assert(!userInput.toString.contains("mongodb://localhost:27017"))
  }

}
