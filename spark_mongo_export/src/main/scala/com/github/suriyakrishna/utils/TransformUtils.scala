package com.github.suriyakrishna.utils

import org.apache.spark.internal.Logging
import org.apache.spark.sql.DataFrame

@SerialVersionUID(3L)
object TransformUtils extends Serializable with Logging {
  // Method to Transform Source Data
  def transformSourceDF(SQL: String): DataFrame => DataFrame = {
    sourceDF => {
      sourceDF.createTempView("source_view")
      logInfo(s"View with name 'source_view' created to use in transformation")
      logInfo(s"Transformation Query\n${SQL}")
      sourceDF.sparkSession.sql(SQL)
    }
  }
}
