package com.github.suriyakrishna

import com.github.suriyakrishna.utils.com.github.suriyakrishna.utils.TimeUtils
import com.github.suriyakrishna.utils.{ReadUtils, TransformUtils, WriteUtils}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

@SerialVersionUID(1L)
object MongoExport extends Serializable with Logging {
  System.setProperty("hadoop.home.dir", "c:\\winutils")

  def main(args: Array[String]): Unit = {
    // Todo :-
    //  clean up the input before using in further steps
    val mongoUri = "mongodb://localhost:27017".trim
    val dbName = "test".trim
    val collectionName = "spark_test".trim
    val writeMode = "overwrite".trim
    val schemaPath = "C:\\Users\\Kishan\\IdeaProjects\\spark-maven\\src\\main\\resources\\student-schema.json".trim // if present use - else infer schema from file
    val dataLocation = "file:\\\\\\C:\\Users\\Kishan\\PycharmProjects\\python_projects\\csv_to_json\\input\\student.csv".trim
    val fileFormat = "csv".trim
    val transformationSQL =
      """
        |SELECT monotonically_increasing_id() AS _id,
        |map('name', name, 'age', age, 'section', section) AS details,
        |current_timestamp() AS recordInsertTMS
        |FROM
        |source_view
      """.stripMargin

    // Todo :-
    //  Need to handle for dynamic options
    val readOptions: Map[String, String] = Map(
      "header" -> "false",
      "inferSchema" -> "true",
      "sep" -> "~"
    )

    // Todo :-
    //  NEVER ADD URI TO THE WRITE OR READ OPTIONS
    //  NEED TO HANDLE FOR DYNAMIC OPTIONS
    val mongoWriteOptions: Map[String, String] = Map(
      "database" -> dbName,
      "collection" -> collectionName,
      "replaceDocument" -> "true",
      "forceInsert" -> "false"
    )

    val logDecorator: Map[Char, String] = Map(
      '#' -> s"${Array.fill(60)('#').mkString}",
      '-' -> s"${Array.fill(60)('-').mkString}"
    )

    val startTime = TimeUtils.getCurrentEpochTime
    val appName = s"${this.getClass.getName.dropRight(1)}-${dbName}-${collectionName}-${startTime}"

    logInfo(logDecorator('#'))
    logInfo(s"SPARK MONGO EXPORT")
    logInfo(logDecorator('-'))
    logInfo(s"dbName : ${dbName}")
    logInfo(s"collectionName : ${collectionName}")
    logInfo(s"writeMode : ${writeMode}")
    logInfo(s"schemaPath : ${schemaPath}")
    logInfo(s"dataLocation : ${dataLocation}")
    logInfo(s"fileFormat : ${fileFormat}")
    logInfo(logDecorator('-'))

    // Create Spark Session with Mongo DataSource
    implicit val spark: SparkSession = SparkSession.builder()
      .master("local")
      .appName(appName)
      .config("spark.mongodb.output.uri", mongoUri)
      .getOrCreate()

    logInfo(s"Spark ApplicationID : ${spark.sparkContext.applicationId}")

    val schema = ReadUtils.JSONSchemaToStructSchema(schemaPath)

    logInfo(logDecorator('-'))
    logInfo("Creating Source DataFrame")

    // Todo :-
    //  Need to handle conditions
    val df = ReadUtils.createSourceDF(dataLocation = dataLocation, format = fileFormat, readOptions = readOptions, schema = schema)

    logInfo(s"Source DataFrame Schema\n${df.schema.treeString}")
    logInfo(logDecorator('-'))

    logInfo("Applying tranformation on Source DataFrame")
    val transformedDF = df.transform(TransformUtils.transformSourceDF(transformationSQL))

    logInfo(s"Transformed DataFrame Schema\n${transformedDF.schema.treeString}")
    logInfo(logDecorator('-'))

    // Todo :-
    //  Need to handle conditions
    WriteUtils.writeToMongo(dataFrame = transformedDF, writeMode = writeMode, options = mongoWriteOptions)

    val endTime = TimeUtils.getCurrentEpochTime
    val timeTook = TimeUtils.getTimeDiff(startTime, endTime)
    logInfo("SPARK MONGO EXPORT - SUCCESSFUL")
    logInfo(logDecorator('-'))
    logInfo(s"TIME TOOK - ${timeTook}")
    logInfo(logDecorator('#'))
    Thread.sleep(100000000L)

  }

}
