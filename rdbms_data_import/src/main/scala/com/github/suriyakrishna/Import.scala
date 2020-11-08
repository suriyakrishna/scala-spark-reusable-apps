package com.github.suriyakrishna

import com.github.suriyakrishna.inputparser.{Input, InputParser}
import com.github.suriyakrishna.utils.{BoundValues, ReadUtils, Time, WriteUtils}
import org.apache.commons.cli.CommandLine
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}

@SerialVersionUID(1L)
object Import extends Logging with Serializable {
  System.setProperty("hadoop.home.dir", "c:\\winutils")

  // Application Name
  private val appName: String = this.getClass.getName.dropRight(1)

  def main(args: Array[String]): Unit = {

    // Parsing User Input Arguments
    val command: CommandLine = InputParser.getCommandLine(args, appName)
    val input: Input = InputParser.getUserInput(command)

    logInfo("Spark JDBC Import Started")

    val startTime = Time.getCurrentEpochTimeStamp

    logInfo(s"User Input ${input.toString}")

    // Instantiating SparkSession
    //    implicit val spark: SparkSession = SparkSession.builder().appName(s"SparkJDBCImport-${input.tableName}-${startTime}").getOrCreate()
    implicit val spark: SparkSession = SparkSession.builder().appName(s"SparkJDBCImport-${input.tableName}-${startTime}").master("local").getOrCreate()

    logInfo(s"Spark Application ID - ${spark.sparkContext.applicationId}")
    logInfo(s"Spark Application Name - ${spark.sparkContext.appName}")

    // Get Boundary Values for Parallel Import
    val boundaryQueryOptions: Map[String, String] = ReadUtils.getBoundaryQueryJDBCOptions(input)
    val bound: BoundValues = ReadUtils.getBoundary(boundaryQueryOptions, input.splitByColumn)

    // Create DataFrame For JDBC Table
    val dfOptions: Map[String, String] = ReadUtils.getSparkReadJBDCOptions(input, bound)
    val df: DataFrame = ReadUtils.getDF(dfOptions, input.columns)

    logInfo(s"Number of Partitions: ${df.rdd.getNumPartitions}")
    logInfo("Writing data to output location")

    // Write Data to outputLocation
    WriteUtils.write(input, df)

    val endTime = Time.getCurrentEpochTimeStamp
    logInfo("Spark JDBC Import Completed")
    logInfo(s"Time Took: ${Time.getTimeDiff(startTime, endTime)}")
  }
}
