package com.github.suriyakrishna.unit

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

abstract class ApplicationTests extends AnyFunSuite with BeforeAndAfterAll {

  lazy val sharedSpark: SparkSession = SparkSession.builder()
    .master("local")
    .appName("Mongo Export Unit Tests")
    .getOrCreate()

  def setUp(): Unit


}