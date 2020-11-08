package com.github.suriyakrishna.utils

import com.github.suriyakrishna.inputparser.Input
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{AnalysisException, DataFrame, SaveMode}

@SerialVersionUID(3L)
object WriteUtils extends Logging with Serializable {
  def write(input: Input, df: DataFrame): Unit = {
    try {
      if (input.overwrite) {
        df.write.mode(SaveMode.Overwrite).format(input.format).save(input.targetDirectory)
      } else if (input.append) {
        df.write.mode(SaveMode.Append).format(input.format).save(input.targetDirectory)
      } else {
        df.write.format(input.format).save(input.targetDirectory)
      }
    } catch {
      case s: AnalysisException => {
        logError(s"Job Failed with Spark SQL Analysis Exception - ${s.getMessage()}", s.fillInStackTrace())
        System.exit(1)
      }
      case e: Exception => {
        logError(s"Job Failed with Unknown Exception - ${e.getMessage()}", e.fillInStackTrace())
        System.exit(1)
      }
    }
  }
}
