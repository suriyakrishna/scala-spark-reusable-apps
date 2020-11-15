package com.github.suriyakrishna.utils

import com.github.suriyakrishna.ApplicationTests

class BoundValuesTests extends ApplicationTests {

  override def setUp(): Unit = {
    // Nothing to setup
  }

  test("BoundValues should return expected values"){
    val bound: BoundValues = BoundValues("1", "200")
    assertResult("BoundValues(lower=1, upper=200)")(bound.toString)
  }
}
