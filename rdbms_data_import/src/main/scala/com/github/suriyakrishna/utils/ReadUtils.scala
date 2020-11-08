package com.github.suriyakrishna.utils

import com.github.suriyakrishna.inputparser.Input
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions.{col, max, min}
import org.apache.spark.sql.{AnalysisException, DataFrame, SparkSession}

@SerialVersionUID(2L)
object ReadUtils extends Logging with Serializable {
  def getBoundary(options: Map[String, String], partitionColumn: String)(implicit spark: SparkSession): BoundValues = {
    var boundary: BoundValues = null
    try {
      val tableName: String = options("dbtable")
      val splitByColumn: String = partitionColumn
      logInfo(s"Executing Boundary Query on table: $tableName and column: $splitByColumn")
      boundary = spark.read.format("jdbc")
        .options(options)
        .load()
        .select(min(col(splitByColumn)).cast("string").alias("lower"), max(splitByColumn).cast("string").alias("upper"))
        .rdd
        .map(a => BoundValues(a.getString(0), a.getString(1)))
        .first()
      logInfo(s"Lower Boundary: ${boundary.lower}")
      logInfo(s"Upper Boundary: ${boundary.upper}")
      if(boundary.lower == null || boundary.upper == null) {
        throw new RuntimeException("Boundary values are null. Check data in table and rerun.")
      }
    } catch {
      case e: Exception => {
        logError(s"Caught Exception while executing boundary query: ${e.getMessage}", e.fillInStackTrace())
        System.exit(1)
      }
    }
    return boundary
  }

  def getBoundaryQueryJDBCOptions(implicit input: Input): Map[String, String] = Map(
    "driver" -> input.driver,
    "url" -> input.url,
    "user" -> input.username,
    "password" -> input.password,
    "dbtable" -> input.tableName
  )


  def getSparkReadJBDCOptions(input: Input, bound: BoundValues): Map[String, String] = Map[String, String](
    "driver" -> input.driver,
    "url" -> input.url,
    "user" -> input.username,
    "password" -> input.password,
    "dbtable" -> input.tableName,
    "partitionColumn" -> input.splitByColumn,
    "lowerBound" -> bound.lower,
    "upperBound" -> bound.upper,
    "numPartitions" -> input.numPartitions
  )

  def getDF(options: Map[String, String], columns: String)(implicit spark: SparkSession): DataFrame = {
    val df = spark.read.format("jdbc").options(options).load()
    if (columns != "all") {
      val requiredColumns = columns.split(",").map(c => col(c.trim))
      try {
        return df.select(requiredColumns: _*)
      } catch {
        case s: AnalysisException =>{
          logError(s"Caught Spark SQL Analysis Exception: ${s.getSimpleMessage}", s.fillInStackTrace())
          System.exit(1)
        }
      }
    }
    return df
  }

}
