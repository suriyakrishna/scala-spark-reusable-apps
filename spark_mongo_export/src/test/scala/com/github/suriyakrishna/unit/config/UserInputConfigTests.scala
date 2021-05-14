package com.github.suriyakrishna.unit.config

import com.github.suriyakrishna.config.UserInputConfig
import com.github.suriyakrishna.models.UserInput
import com.github.suriyakrishna.unit.ApplicationTests

class UserInputConfigTests extends ApplicationTests {

  private final val MONGO_URI = "mongodb://localhost:27017"
  private final val DB_NAME = "test"
  private final val COLLECTION_NAME = "student"
  private final val SOURCE_DATA_LOCATION = "/user/hadoop/students.csv"
  private final val SOURCE_FILE_FORMAT = "csv"
  private final val WRITE_MODE = "append"
  private final val SCHEMA_FILE = "/home/hadoop/spark_reusable_application/spark_mongo_export/student/student-schema.json"
  private final val TRANSFORM_SQL = "SELECT monotonically_increasing_id() AS _id, map('name', name, 'age', age, 'section', section) AS details, current_timestamp() AS recordInsertTMS FROM source_view"
  private final val READ_OPTIONS = "inferSchema=true'sep=~'header=false"
  private final val WRITE_OPTIONS = "replaceDocument=false'forceInsert=false"
  private final val NUM_PARTITIONS = "1"
  private final val CLASS_NAME = this.getClass.getSimpleName.dropRight(1)

  override def setUp(): Unit = {
    // nothing to setUp
  }

  test("UserInputConfig Should return UserInput Object for Valid Input Options --longOptions") {
    val userArgs: Array[String] = Array(
      "--mongoURI", MONGO_URI,
      "--dbName", DB_NAME,
      "--collectionName", COLLECTION_NAME,
      "--dataLocation", SOURCE_DATA_LOCATION,
      "--fileFormat", SOURCE_FILE_FORMAT,
      "--writeMode", WRITE_MODE,
      "--schemaPath", SCHEMA_FILE,
      "--transformationSQL", TRANSFORM_SQL,
      "--writeOptions", WRITE_OPTIONS,
      "--readOptions", READ_OPTIONS,
      "--numPartitions", NUM_PARTITIONS
    )

    val userInput: UserInput = UserInputConfig(userArgs, CLASS_NAME)

    assertResult(MONGO_URI)(userInput.mongoURI)
    assertResult(DB_NAME)(userInput.dbName)
    assertResult(COLLECTION_NAME)(userInput.collectionName)
    assertResult(SOURCE_DATA_LOCATION)(userInput.dataLocation)
    assertResult(SOURCE_FILE_FORMAT)(userInput.fileFormat)
    assertResult(WRITE_MODE)(userInput.writeMode)
    assertResult(SCHEMA_FILE)(userInput.schemaPath)
    assertResult(TRANSFORM_SQL)(userInput.transformationSQL)
    assert(Map("inferSchema" -> "true", "sep" -> "~", "header" -> "false").equals(userInput.readOptions))
    assert(Map("collection" -> COLLECTION_NAME, "database" -> DB_NAME, "replaceDocument" -> "false", "forceInsert" -> "false").equals(userInput.writeOptions))
    assertResult(NUM_PARTITIONS.toInt)(userInput.numPartitions)

  }

  test("UserInputConfig Should return UserInput Object for Valid Input Options -shortOptions") {
    val userArgs: Array[String] = Array(
      "-uri", MONGO_URI,
      "-db", DB_NAME,
      "-cName", COLLECTION_NAME,
      "-dLocation", SOURCE_DATA_LOCATION,
      "-fFormat", SOURCE_FILE_FORMAT,
      "-wMode", WRITE_MODE,
      "-sPath", SCHEMA_FILE,
      "-tSQL", TRANSFORM_SQL,
      "-wOptions", WRITE_OPTIONS,
      "-rOptions", READ_OPTIONS,
      "--nPartitions", NUM_PARTITIONS
    )

    val userInput: UserInput = UserInputConfig(userArgs, CLASS_NAME)

    assertResult(MONGO_URI)(userInput.mongoURI)
    assertResult(DB_NAME)(userInput.dbName)
    assertResult(COLLECTION_NAME)(userInput.collectionName)
    assertResult(SOURCE_DATA_LOCATION)(userInput.dataLocation)
    assertResult(SOURCE_FILE_FORMAT)(userInput.fileFormat)
    assertResult(WRITE_MODE)(userInput.writeMode)
    assertResult(SCHEMA_FILE)(userInput.schemaPath)
    assertResult(TRANSFORM_SQL)(userInput.transformationSQL)
    assert(Map("inferSchema" -> "true", "sep" -> "~", "header" -> "false").equals(userInput.readOptions))
    assert(Map("collection" -> COLLECTION_NAME, "database" -> DB_NAME, "replaceDocument" -> "false", "forceInsert" -> "false").equals(userInput.writeOptions))
    assertResult(NUM_PARTITIONS.toInt)(userInput.numPartitions)

  }

  test("UserInputConfig Should throw Runtime when wMode is neither overwrite or append") {
    val userArgs: Array[String] = Array(
      "-uri", MONGO_URI,
      "-db", DB_NAME,
      "-cName", COLLECTION_NAME,
      "-dLocation", SOURCE_DATA_LOCATION,
      "-fFormat", SOURCE_FILE_FORMAT,
      "-wMode", "test",
      "-sPath", SCHEMA_FILE,
      "-tSQL", TRANSFORM_SQL,
      "-wOptions", WRITE_OPTIONS,
      "-rOptions", READ_OPTIONS,
      "--nPartitions", NUM_PARTITIONS
    )

    assertThrows[RuntimeException] {
      UserInputConfig(userArgs, CLASS_NAME)
    }
  }

  test("UserInputConfig Should assign default value for optional options") {
    val userArgs: Array[String] = Array(
      "-uri", MONGO_URI,
      "-db", DB_NAME,
      "-cName", COLLECTION_NAME,
      "-dLocation", SOURCE_DATA_LOCATION,
      "-fFormat", SOURCE_FILE_FORMAT
    )


    val userInput: UserInput = UserInputConfig(userArgs, CLASS_NAME)

    assertResult(MONGO_URI)(userInput.mongoURI)
    assertResult(DB_NAME)(userInput.dbName)
    assertResult(COLLECTION_NAME)(userInput.collectionName)
    assertResult(SOURCE_DATA_LOCATION)(userInput.dataLocation)
    assertResult(SOURCE_FILE_FORMAT)(userInput.fileFormat)
    assertResult("append")(userInput.writeMode)
    assertResult(null)(userInput.schemaPath)
    assertResult(null)(userInput.transformationSQL)
    assertResult(null)(userInput.readOptions)
    assert(Map("collection" -> COLLECTION_NAME, "database" -> DB_NAME).equals(userInput.writeOptions))
    assertResult(10)(userInput.numPartitions)

  }
}
