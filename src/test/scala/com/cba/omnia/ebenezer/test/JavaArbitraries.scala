package com.cba.omnia.ebenezer
package test

import java.util.UUID
import org.scalacheck._, Gen._, Arbitrary._

object JavaArbitraries {
  implicit def ArbitraryUUID: Arbitrary[UUID] =
    Arbitrary(arbitrary[Int] map (_ => UUID.randomUUID))
}
