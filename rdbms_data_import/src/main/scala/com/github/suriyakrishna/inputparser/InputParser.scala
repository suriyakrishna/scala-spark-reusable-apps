package com.github.suriyakrishna.inputparser

import org.apache.commons.cli.{CommandLine, HelpFormatter, MissingArgumentException, MissingOptionException, Option, Options, PosixParser}


object InputParser {

  def getUserInput(command: CommandLine): Input = {
    val driver: String = command.getOptionValue("driver")
    val url: String = command.getOptionValue("url")
    val userName: String = command.getOptionValue("username")
    val password: String = command.getOptionValue("password")
    val table: String = command.getOptionValue("table")
    val splitBy: String = command.getOptionValue("splitBy")
    val targetDirectory: String = command.getOptionValue("target-dir")
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
    if (command.hasOption("numPartitions")) {
      numPartitions = command.getOptionValue("numPartitions")
    }
    var format: String = "parquet"
    if (command.hasOption("format")) {
      format = command.getOptionValue("format").toLowerCase
      if(!(format == "json" || format == "parquet" || format == "csv")) {
        throw new RuntimeException(s"Output format cannot be ${format.toUpperCase}. It can be only JSON/PARQUET/CSV.")
      }
    }
    var columns: String = "all"
    if (command.hasOption("columns")) {
      columns = command.getOptionValue("columns")
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
      columns
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
    val splitBy: Option = new Option("s", "splitBy", true, "splitBy Column for parallelism")
    splitBy.setRequired(true)
    val targetDirectory: Option = new Option("target", "target-dir", true, "Target directory to import data")
    targetDirectory.setRequired(true)
    val append: Option = new Option("a", "append", false, "Flag to set append")
    val overwrite: Option = new Option("o", "overwrite", false, "Flag to set overwrite")
    val numPartitions: Option = new Option("n", "numPartitions", true, "Number of partitions for parallelism. By default 4")
    val outputFormat: Option = new Option("f", "format", true, "Output file format. By default parquet. Can be CSV/PARQUET/JSON")
    val columns: Option = new Option("c", "columns", true, "List of columns to import separated by comma")

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
    return options
  }

}
