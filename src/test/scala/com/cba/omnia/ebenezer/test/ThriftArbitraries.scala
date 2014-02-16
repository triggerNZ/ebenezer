package com.cba.omnia.ebenezer
package test

import org.scalacheck._, Gen._, Arbitrary._

object ThriftArbitraries {
  implicit def NestedArbitrary: Arbitrary[Nested] =
    Arbitrary(arbitrary[Utf8String] map (s => Nested(s.value)))

  implicit def SomethingOrOtherArbitrary: Arbitrary[SomethingOrOther] =
    Arbitrary(oneOf(SomethingOrOther.Some, SomethingOrOther.Other))

  implicit def BoolishArbitrary: Arbitrary[Boolish] =
    Arbitrary(arbitrary[Boolean] map Boolish.apply)

  implicit def DoublishArbitrary: Arbitrary[Doublish] =
    Arbitrary(arbitrary[Double] map Doublish.apply)

  implicit def BytishArbitrary: Arbitrary[Bytish] =
    Arbitrary(arbitrary[Byte] map Bytish.apply)

  implicit def ShortishArbitrary: Arbitrary[Shortish] =
    Arbitrary(arbitrary[Short] map Shortish.apply)

  implicit def IntishArbitrary: Arbitrary[Intish] =
    Arbitrary(arbitrary[Int] map Intish.apply)

  implicit def LongishArbitrary: Arbitrary[Longish] =
    Arbitrary(arbitrary[Long] map Longish.apply)

  implicit def StringishArbitrary: Arbitrary[Stringish] =
    Arbitrary(arbitrary[Utf8String] map (s => Stringish(s.value)))

  implicit def NestedishArbitrary: Arbitrary[Nestedish] =
    Arbitrary(arbitrary[Nested] map Nestedish.apply)

  implicit def ListishArbitrary: Arbitrary[Listish] =
    Arbitrary(arbitrary[List[Utf8String]] map (ss => Listish(ss.map(_.value))))

  implicit def MapishArbitrary: Arbitrary[Mapish] =
    Arbitrary(arbitrary[List[(Utf8String, Utf8String)]] map (ss => Mapish(Map(ss.take(10).map({ case (k, v) => (k.value, v.value) }):_*))))

  implicit def EnumishArbitrary: Arbitrary[Enumish] =
    Arbitrary(arbitrary[SomethingOrOther] map Enumish.apply)

  implicit def CustomerArbitrary: Arbitrary[Customer] =
    Arbitrary(for {
      id <- arbitrary[Utf8String]
      name <- arbitrary[Utf8String]
      address <- arbitrary[Utf8String]
      age <- arbitrary[Int]
    } yield Customer(id.value, name.value, address.value, age))
}
