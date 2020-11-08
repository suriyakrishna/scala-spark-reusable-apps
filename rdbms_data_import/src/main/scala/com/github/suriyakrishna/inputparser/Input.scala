package com.github.suriyakrishna.inputparser

case class Input(driver: String,
                 url: String,
                 username: String,
                 password: String,
                 tableName: String,
                 splitByColumn: String,
                 targetDirectory: String,
                 append: Boolean,
                 overwrite: Boolean,
                 numPartitions: String,
                 format: String,
                 columns: String) {
  override def toString: String = {
    s"""
      ${Array.fill[String](10)("#").mkString} User Input ${Array.fill[String](10)("#").mkString}
      driver: ${driver}
      url: ${url}
      tableName: ${tableName}
      splitByColumn: ${splitByColumn}
      targetDirectory: ${targetDirectory}
      append: ${append}
      overwrite: ${overwrite}
      numPartitions: ${numPartitions}
      format: ${format}
      columns: ${columns}
      ${Array.fill[String](31)("#").mkString}
    """
  }
}

