package com.github.suriyakrishna.utils

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SaveMode}

@SerialVersionUID(4L)
object WriteUtils extends Serializable with Logging {
  // Write Data To MongoDB
  def writeToMongo(dataFrame: DataFrame, writeMode: String, options: Map[String, String]): Unit = {
    val dataFrameWriter: SaveMode => Unit = saveMode => {
      try {
        dataFrame.write
          .mode(saveMode)
          .format("mongo")
          .options(options)
          .save()
      } catch {
        case e: Exception => {
          val message = s"Caught '${e.getMessage}' exception while exporting data to MongoDB"
          logError(message, e.getCause)
          throw new RuntimeException(message)
        }
      }
    }

    writeMode.trim.toLowerCase() match {
      case "overwrite" => dataFrameWriter(SaveMode.Overwrite)
      case "append" => dataFrameWriter(SaveMode.Append)
      case _ => {
        throw new RuntimeException(s"Write Mode Not Supported ${writeMode.trim}")
      }
    }
  }
}
