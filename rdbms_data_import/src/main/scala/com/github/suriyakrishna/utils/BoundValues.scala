package com.github.suriyakrishna.utils

case class BoundValues(lower: String,
                       upper: String) {
  override def toString: String = {
    s"BoundValues(lower=${lower}, upper=${upper})"
  }
}
