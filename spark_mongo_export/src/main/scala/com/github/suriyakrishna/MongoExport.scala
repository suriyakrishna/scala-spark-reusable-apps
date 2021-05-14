package com.github.suriyakrishna

import com.github.suriyakrishna.config.UserInputConfig
import com.github.suriyakrishna.models.UserInput
import com.github.suriyakrishna.utils.{ReadUtils, TimeUtils, TransformUtils, WriteUtils}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

@SerialVersionUID(1L)
object MongoExport extends Serializable with Logging {

  // Todo :-
  //  Remove this property - Kept for testing the application in IntelliJ
  System.setProperty("hadoop.home.dir", "c:\\winutils")

  private val appClassName: String = this.getClass.getName.dropRight(1)

  private val logDecorator: Map[Char, String] = Map(
    '#' -> s"${Array.fill(60)('#').mkString}",
    '-' -> s"${Array.fill(60)('-').mkString}"
  )

  def main(args: Array[String]): Unit = {

    val userInput: UserInput = UserInputConfig(args, appClassName)

    val startTime = TimeUtils.getCurrentEpochTime
    val appName = s"Spark-Mongo-Export-${userInput.dbName}-${userInput.collectionName}-${TimeUtils.fromEpoch(startTime, "yyyy-MM-dd_HH-mm-ss")}"

    logInfo(logDecorator('#'))
    logInfo(s"SPARK MONGO EXPORT - ${appName}")
    logInfo(logDecorator('-'))
    logInfo(userInput.toString)
    logInfo(logDecorator('-'))

    // Todo :-
    //  Remove master from SparkSession Builder
    // Create Spark Session with Mongo DataSource
    implicit val spark: SparkSession = SparkSession.builder()
      .master("local")
      .appName(appName)
      .config("spark.mongodb.output.uri", userInput.mongoURI)
      .getOrCreate()

    logInfo(s"Spark ApplicationID : ${spark.sparkContext.applicationId}")
    var schema: StructType = null
    if (userInput.schemaPath != null) {
      schema = ReadUtils.JSONSchemaToStructSchema(userInput.schemaPath)
    }

    logInfo(logDecorator('-'))
    logInfo("Creating Source DataFrame")

    var df = ReadUtils.createSourceDF(dataLocation = userInput.dataLocation, format = userInput.fileFormat, readOptions = userInput.readOptions, schema = schema)

    logInfo(s"Source DataFrame Schema\n${df.schema.treeString}")
    logInfo(logDecorator('-'))

    var resultDF: DataFrame = df
    if (userInput.transformationSQL != null) {
      logInfo("Applying transformation on Source DataFrame")
      resultDF = df.transform(TransformUtils.transformSourceDF(userInput.transformationSQL))
      logInfo(s"Transformed DataFrame Schema\n${resultDF.schema.treeString}")
      logInfo(logDecorator('-'))
    }

    logInfo("Writing data to MongoDB")
    WriteUtils.writeToMongo(dataFrame = resultDF, writeMode = userInput.writeMode, numPartitions = userInput.numPartitions, options = userInput.writeOptions)
    logInfo("Write Completed")

    val endTime = TimeUtils.getCurrentEpochTime
    val timeTook = TimeUtils.getTimeDiff(startTime, endTime)

    logInfo(logDecorator('-'))
    logInfo("SPARK MONGO EXPORT - SUCCESSFUL")
    logInfo(logDecorator('-'))
    logInfo(s"TIME TOOK - ${timeTook}")
    logInfo(logDecorator('#'))

    // Todo :-
    //  Remove sleep - Kept for debugging in spark application console
    Thread.sleep(100000000L)

  }

}
