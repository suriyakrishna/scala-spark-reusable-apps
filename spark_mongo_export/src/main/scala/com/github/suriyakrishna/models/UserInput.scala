package com.github.suriyakrishna.models

case class UserInput(mongoURI: String,
                     dbName: String,
                     collectionName: String,
                     dataLocation: String,
                     fileFormat: String,
                     writeMode: String,
                     schemaPath: String,
                     transformationSQL: String,
                     readOptions: Map[String, String],
                     writeOptions: Map[String, String],
                     numPartitions: Int) {
  override def toString: String = {
    s"""
       |${Array.fill(20)('#').mkString} User Input ${Array.fill(20)('#').mkString}
       |dbName            : $dbName
       |collectionName    : $collectionName
       |dataLocation      : $dataLocation
       |fileFormat        : $fileFormat
       |writeMode         : $writeMode
       |schemaPath        : $schemaPath
       |transformationSQL : $transformationSQL
       |readOptions       : $readOptions
       |writeOptions      : $writeOptions
       |numPartitions     : $numPartitions
       |${Array.fill(52)('#').mkString}
    """.stripMargin
  }
}
