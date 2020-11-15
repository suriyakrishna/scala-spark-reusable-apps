package com.github.suriyakrishna.inputparser

import org.apache.commons.cli.{CommandLine, HelpFormatter, MissingArgumentException, MissingOptionException, Option, Options, PosixParser}


object InputParser {

  def getUserInput(command: CommandLine): Input = {
    val driver: String = command.getOptionValue("driver").trim
    val url: String = command.getOptionValue("url").trim
    val userName: String = command.getOptionValue("username").trim
    val password: String = command.getOptionValue("password").trim
    val table: String = command.getOptionValue("table").trim
    val splitBy: String = command.getOptionValue("split-by").trim
    val targetDirectory: String = command.getOptionValue("target-dir").trim
    var append: Boolean = false
    if (command.hasOption("append")) {
      append = true
    }
    var overwrite: Boolean = false
    if (command.hasOption("overwrite")) {
      overwrite = true
    }
    if (append && overwrite) {
      throw new RuntimeException("--append and --overwrite both options cannot be used together")
    }
    var numPartitions: String = "4"
    if (command.hasOption("num-partitions")) {
      numPartitions = command.getOptionValue("num-partitions").trim
    }
    var format: String = "parquet"
    if (command.hasOption("format")) {
      format = command.getOptionValue("format").trim.toLowerCase
      // Validation for Output File Format Type
      if (!(format == "json" || format == "parquet" || format == "csv")) {
        throw new RuntimeException(s"Output format cannot be ${format.toUpperCase}. It can be only JSON/PARQUET/CSV.")
      }
    }
    var columns: String = "all"
    if (command.hasOption("columns")) {
      columns = command.getOptionValue("columns").trim
    }
    var incremental: Boolean = false
    if (command.hasOption("incremental")) {
      incremental = true
      // Validation for Incremental
      if (incremental && overwrite) {
        throw new RuntimeException(s"--overwrite option cannot be used with --incremental option")
      }
      if (incremental && !append) {
        throw new RuntimeException(s"--incremental option cannot be used without --append option")
      }
      if (incremental && !command.hasOption("incremental-type")) {
        throw new RuntimeException(s"--incremental-type option should be specified with --incremental option.")
      }
      if (incremental && !command.hasOption("incremental-column")) {
        throw new RuntimeException(s"--incremental-column option should be specified with --incremental option.")
      }
    }
    var incrementalType: String = null
    if (command.hasOption("incremental-type")) {
      incrementalType = command.getOptionValue("incremental-type").trim.toLowerCase
      // Validation for Incremental Type
      if (!(incrementalType == "id" || incrementalType == "timestamp")) {
        throw new RuntimeException(s"Incremental import type cannot be ${incrementalType.toUpperCase}. It can be only ID/TIMESTAMP.")
      }
      // Validation for ID option and TIMESTAMP options not be used together
      if (command.hasOption("incremental-id") && (command.hasOption("incremental-time-format") || command.hasOption("incremental-start-time") || command.hasOption("incremental-end-time"))) {
        throw new RuntimeException(s"--incremental-id should not be used with --incremental-time-format, --incremental-start-time and --incremental-end-time options.")
      }
      // Validation for incremental type ID
      if (incrementalType == "id" && !command.hasOption("incremental-id")) {
        throw new RuntimeException(s"--incremental-id option should be specified for option --incremental-type 'ID'")
      }
      // Validation for incremental type TIMESTAMP
      if (incrementalType == "timestamp" && (!command.hasOption("incremental-time-format") || !command.hasOption("incremental-start-time") || !command.hasOption("incremental-end-time"))) {
        throw new RuntimeException(s"--incremental-time-format, --incremental-start-time and --incremental-end-time options should be specified for option --incremental-type 'TIMESTAMP'")
      }
    }
    var incrementalColumn: String = null
    if (command.hasOption("incremental-column")) {
      incrementalColumn = command.getOptionValue("incremental-column").trim
    }
    var incrementalIdValue: String = null
    if (command.hasOption("incremental-id")) {
      incrementalIdValue = command.getOptionValue("incremental-id").trim
    }
    var incrementalTimeFormat: String = null
    if (command.hasOption("incremental-time-format")) {
      incrementalTimeFormat = command.getOptionValue("incremental-time-format").trim
    }
    var incrementalStartTime: String = null
    if (command.hasOption("incremental-start-time")) {
      incrementalStartTime = command.getOptionValue("incremental-start-time").trim
    }
    var incrementalEndTime: String = null
    if (command.hasOption("incremental-end-time")) {
      incrementalEndTime = command.getOptionValue("incremental-end-time").trim
    }

    Input(
      driver,
      url,
      userName,
      password,
      table,
      splitBy,
      targetDirectory,
      append,
      overwrite,
      numPartitions,
      format,
      columns,
      incremental,
      incrementalType,
      incrementalColumn,
      incrementalIdValue,
      incrementalTimeFormat,
      incrementalStartTime,
      incrementalEndTime
    )
  }

  def getCommandLine(args: Array[String], appName: String): CommandLine = {
    val options: Options = getOptions
    val parser: PosixParser = new PosixParser()
    var commands: CommandLine = null
    try {
      commands = parser.parse(options, args)
    } catch {
      case e: MissingOptionException => {
        println(s"Missing required options: ${e.getMissingOptions}\n")
        help(appName, options)
        System.exit(1)
      }
      case a: MissingArgumentException => {
        println(s"Missing Argument for option: ${a.getOption.getLongOpt}\n")
        help(appName, options)
        System.exit(1)
      }
    }
    return commands
  }

  private def help(appname: String, appOptions: Options): Unit = {
    val helpFormatted: HelpFormatter = new HelpFormatter()
    val header = "\nSpark JDBC Import"
    val footer = "\nContact: suriya.kishan@live.com"
    helpFormatted.printHelp(appname, header, appOptions, footer, true)
  }

  private def getOptions: Options = {

    // Define Application Options
    val driver: Option = new Option("d", "driver", true, "JDBC/ODBC Driver Path")
    driver.setRequired(true)
    val url: Option = new Option("url", "url", true, "JDBC/ODBC url")
    url.setRequired(true)
    val userName: Option = new Option("uName", "username", true, "JDBC/ODBC username")
    userName.setRequired(true)
    val password: Option = new Option("pass", "password", true, "JDBC/ODBC password")
    password.setRequired(true)
    val table: Option = new Option("t", "table", true, "JDBC/ODBC table name")
    table.setRequired(true)
    val splitBy: Option = new Option("s", "split-by", true, "splitBy Column for parallelism")
    splitBy.setRequired(true)
    val targetDirectory: Option = new Option("target", "target-dir", true, "Target directory to import data")
    targetDirectory.setRequired(true)
    val append: Option = new Option("a", "append", false, "Flag to set append")
    val overwrite: Option = new Option("o", "overwrite", false, "Flag to set overwrite")
    val numPartitions: Option = new Option("n", "num-partitions", true, "Number of partitions for parallelism. By default 4")
    val outputFormat: Option = new Option("f", "format", true, "Output file format. By default parquet. Can be CSV/PARQUET/JSON")
    val columns: Option = new Option("c", "columns", true, "List of columns to import separated by comma")
    val incremental: Option = new Option("inc", "incremental", false, "Flag to set incremental import")
    val incrementalType: Option = new Option("incType", "incremental-type", true, "Type of incremental import ID/TIMESTAMP")
    val incrementalColumn: Option = new Option("incColumn", "incremental-column", true, "Column to be used for incremental import")
    val incrementalIdValue: Option = new Option("incId", "incremental-id", true, "Value of Start ID for incremental import. To be used when incremental-type is 'ID'.")
    val incrementalTimeFormat: Option = new Option("incFormat", "incremental-time-format", true, "Timestamp format representation in string. To be used when incremental-type is 'TIMESTAMP'.")
    val incrementalStartTime: Option = new Option("incStart", "incremental-start-time", true, "Start timestamp value. To be used when incremental-type is 'TIMESTAMP'.")
    val incrementalEndTime: Option = new Option("incEnd", "incremental-end-time", true, "End timestamp Value. To be used when incremental-type is 'TIMESTAMP'.")

    val options: Options = new Options()
    options.addOption(driver)
    options.addOption(url)
    options.addOption(userName)
    options.addOption(password)
    options.addOption(table)
    options.addOption(splitBy)
    options.addOption(targetDirectory)
    options.addOption(append)
    options.addOption(overwrite)
    options.addOption(numPartitions)
    options.addOption(outputFormat)
    options.addOption(columns)
    options.addOption(incremental)
    options.addOption(incrementalType)
    options.addOption(incrementalColumn)
    options.addOption(incrementalIdValue)
    options.addOption(incrementalTimeFormat)
    options.addOption(incrementalStartTime)
    options.addOption(incrementalEndTime)
    return options
  }

}
