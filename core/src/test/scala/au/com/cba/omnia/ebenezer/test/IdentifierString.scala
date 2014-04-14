package au.com.cba.omnia.ebenezer
package test

import org.scalacheck._, Gen._, Arbitrary._

/* A random UTF-8 string, that only uses identifier chars, this is very useful for debugging. */
case class IdentifierString(value: String)

object IdentifierString {
  implicit val ArbitraryIdentifierString: Arbitrary[IdentifierString] =
    Arbitrary(Gen.identifier map (IdentifierString.apply))
}
