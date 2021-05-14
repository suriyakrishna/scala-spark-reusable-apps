package com.github.suriyakrishna.config

import com.github.suriyakrishna.models.UserInput
import org.apache.commons.cli.{CommandLine, HelpFormatter, MissingArgumentException, MissingOptionException, Option, Options, PosixParser}

class UserInputConfig private(args: Array[String], className: String) {

  def getUserInput: UserInput = {
    val commandLine: CommandLine = getCommandLine(args, className)

    // Sanity check for option value
    // Mandatory Options
    val mongoURI: String = commandLine.getOptionValue("mongoURI").trim
    val dbName: String = commandLine.getOptionValue("dbName").trim
    val collectionName: String = commandLine.getOptionValue("collectionName").trim
    val dataLocation: String = commandLine.getOptionValue("dataLocation").trim
    val fileFormat: String = commandLine.getOptionValue("fileFormat").trim.toLowerCase

    // Optional Options
    var writeMode: String = "append"
    if (commandLine.hasOption("writeMode")) {
      writeMode = commandLine.getOptionValue("writeMode").trim.toLowerCase()
      if (!(writeMode == "overwrite" || writeMode == "append")) {
        throw new RuntimeException(s"MongoDB Write Mode should be either overwrite or append. But provided ${writeMode}")
      }
    }

    var schemaPath: String = null
    if (commandLine.hasOption("schemaPath")) {
      schemaPath = commandLine.getOptionValue("schemaPath").trim
    }

    var transformationSQL: String = null
    if (commandLine.hasOption("transformationSQL")) {
      transformationSQL = commandLine.getOptionValue("transformationSQL").trim
    }

    var readOptions: Map[String, String] = null
    if (commandLine.hasOption("readOptions")) {
      val readOptionsString = commandLine.getOptionValue("readOptions").trim
      readOptions = optionsStringToMap(readOptionsString)
    }

    var writeOptions: Map[String, String] = Map(
      "database" -> dbName,
      "collection" -> collectionName
    )
    if (commandLine.hasOption("writeOptions")) {
      val writeOptionsString = commandLine.getOptionValue("writeOptions").trim
      writeOptions = writeOptions ++ optionsStringToMap(writeOptionsString)
    }

    var numPartitions: Int = 10
    if (commandLine.hasOption("numPartitions")) {
      numPartitions = commandLine.getOptionValue("numPartitions").trim.toInt
    }

    // return UserInput
    UserInput(
      mongoURI = mongoURI,
      dbName = dbName,
      collectionName = collectionName,
      dataLocation = dataLocation,
      fileFormat = fileFormat,
      writeMode = writeMode,
      schemaPath = schemaPath,
      transformationSQL = transformationSQL,
      readOptions = readOptions,
      writeOptions = writeOptions,
      numPartitions = numPartitions
    )
  }

  private def getCommandLine(args: Array[String], appName: String): CommandLine = {
    val options: Options = getAppOptions
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
    // return commands
    commands
  }

  private def help(appname: String, appOptions: Options): Unit = {
    val helpFormatted: HelpFormatter = new HelpFormatter()
    val header = "\nSpark Mongo Export"
    val footer = "\nContact: suriya.kishan@live.com"
    helpFormatted.printHelp(appname, header, appOptions, footer, true)
  }

  private def getAppOptions: Options = {
    val mongoURI: Option = new Option("uri", "mongoURI", true, "MongoDB URI")
    mongoURI.setRequired(true)
    val dbName: Option = new Option("db", "dbName", true, "MongoDB Database Name")
    dbName.setRequired(true)
    val collectionName: Option = new Option("cName", "collectionName", true, "MongoDB Collection Name")
    collectionName.setRequired(true)
    val dataLocation: Option = new Option("dLocation", "dataLocation", true, "Source data location")
    dataLocation.setRequired(true)
    val fileFormat: Option = new Option("fFormat", "fileFormat", true, "Source Data File Format")
    fileFormat.setRequired(true)
    val writeMode: Option = new Option("wMode", "writeMode", true, "Either overwrite or append. Default append i.e. Upsert based on _id if shardKey option is not provided")
    val schemaPath: Option = new Option("sPath", "schemaPath", true, "Input Data File Schema Path. Schema will be inferred from file by default.")
    val transformationSQL: Option = new Option("tSQL", "transformationSQL", true, "SQL to transform the source data before writing to MongoDB. Use view name as 'source_view'")
    val readOptions: Option = new Option("rOptions", "readOptions", true, "Spark DF Read Options, case sensitive!. Eg: inferSchema:true'sep:~'header:false")
    val writeOptions: Option = new Option("wOptions", "writeOptions", true, "Spark Mongo DF Write Options, case sensitive!. Eg: replaceDocument:true'forceInsert:false")
    val numPartitions: Option = new Option("nPartitions", "numPartitions", true, "Parallelism to write to MongoDB. Default 10")

    val options: Options = new Options()

    // Mandatory Options
    options.addOption(mongoURI)
    options.addOption(dbName)
    options.addOption(collectionName)
    options.addOption(dataLocation)
    options.addOption(fileFormat)

    // Optional Options
    options.addOption(writeMode)
    options.addOption(schemaPath)
    options.addOption(transformationSQL)
    options.addOption(readOptions)
    options.addOption(writeOptions)
    options.addOption(numPartitions)

    // return options
    options
  }

  private def optionsStringToMap(options: String): Map[String, String] = {
    options.split("'")
      .map(option => {
        val t = option.split("=")
        (t.head.trim, t.last.trim)
      }).toMap
  }
}

object UserInputConfig {
  def apply(args: Array[String], className: String): UserInput = {
    val userInputConfig: UserInputConfig = new UserInputConfig(args, className)
    userInputConfig.getUserInput
  }
}
