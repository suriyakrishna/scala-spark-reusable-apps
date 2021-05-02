package com.github.suriyakrishna.utils

import org.apache.spark.internal.Logging
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{DataFrame, DataFrameReader, SparkSession}

import scala.io.Source

@SerialVersionUID(2L)
object ReadUtils extends Serializable with Logging {
  // SPARK JSON Schema to StructType
  def JSONSchemaToStructSchema(filePath: String): StructType = {

    // Validate dataLocation
    if (filePath.isEmpty || filePath == null) {
      throw new RuntimeException("File Path for Spark JSON Schema can't be null or empty string")
    }

    // Read File and Get contents
    val src = Source.fromFile(filePath)
    val schemaString = src.getLines().mkString
    src.close()

    // Convert JSON to StructType
    DataType.fromJson(schemaString).asInstanceOf[StructType]
  }

  // Create DataFrame
  def createSourceDF(dataLocation: String, format: String, readOptions: Map[String, String] = Map(), schema: StructType = null)(implicit spark: SparkSession): DataFrame = {
    // Validate dataLocation
    if (dataLocation.isEmpty || dataLocation == null) {
      throw new RuntimeException("Data Location can't be null or empty string")
    }

    // Apply Schema If Provided
    val readerWithSchema: DataFrameReader = {
      schema match {
        case null => spark.read
        case _ => spark.read.schema(schema)
      }
    }

    // Apply Options If Provided and Return Function to Accept File Format
    var reader: String => DataFrame = fileFormat => {
      if (readOptions.isEmpty) {
        readerWithSchema.format(fileFormat).load(dataLocation)
      } else {
        readerWithSchema.options(readOptions).format(fileFormat).load(dataLocation)
      }
    }

    // Finally Create DataFrame
    format.trim.toLowerCase() match {
      case "csv" => reader("csv")
      case "parquet" => reader("parquet")
      case "json" => reader("json")
      case _ => {
        throw new RuntimeException(s"File Format Not Supported ${format.trim}")
      }
    }
  }
}
