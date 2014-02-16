package com.cba.omnia.ebenezer
package test

import org.scalacheck._, Gen._, Arbitrary._

/* A random string containing only valid UTF-8 characters. */
case class Utf8String(value: String)

object Utf8String {
  implicit val ArbitraryUtf8String: Arbitrary[Utf8String] =
    Arbitrary(arbitrary[String] map (k =>
      Utf8String(k filter (c => c < '\ud800' || c > '\udfff'))))
}
