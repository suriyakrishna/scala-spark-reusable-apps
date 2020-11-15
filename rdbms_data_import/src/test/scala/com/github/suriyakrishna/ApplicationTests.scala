package com.github.suriyakrishna

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

abstract class ApplicationTests extends AnyFunSuite with BeforeAndAfterAll {

  def setUp(): Unit

}
