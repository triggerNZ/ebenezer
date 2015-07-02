//   Copyright 2015 Commonwealth Bank of Australia
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.

package au.com.cba.omnia.ebenezer
package scrooge

import com.twitter.scalding.TDsl._
import com.twitter.scalding._
import com.twitter.scalding.typed.IterablePipe

import au.com.cba.omnia.thermometer.core.Thermometer._
import au.com.cba.omnia.thermometer.core.ThermometerSpec
import au.com.cba.omnia.thermometer.fact.PathFactoids._

import au.com.cba.omnia.ebenezer.ParquetLogging
import au.com.cba.omnia.ebenezer.scrooge.ParquetScroogeSourceSpec._
import au.com.cba.omnia.ebenezer.test.Customers.{NewCustomer, OldCustomer}
import au.com.cba.omnia.ebenezer.test.NestedCustomers.NestedA
import au.com.cba.omnia.ebenezer.test.WideUnion._
import au.com.cba.omnia.ebenezer.test._

object Unions  extends ThermometerSpec with ParquetLogging { def is = s2"""

Parquet with Unions
===================

  Write union to parquet                      $writeU
  Read union from parquet                     $readU
  Write wide union to parquet                 $writeWide
  Write and read nested unions                $writeReadNested
"""

  val doubleCusts: List[Customers] =
    data.flatMap(cust => List(OldCustomer(cust), NewCustomer(customerNew(cust))) )

  val nestedCusts: List[NestedCustomers] =
    doubleCusts.map(c => NestedA(c))

  val structCusts: List[UnionStruct] =
    nestedCusts.map( c => UnionStruct(AOrB.A, Some(c)))

  // data for the wide union (99 i32 fields a1..a99)
  val wideData: List[WideUnion] = List(
     A1(1),A2(2),A3(3),A4(4),A5(5),A6(6),A7(7),A8(8),A9(9)
    ,A10(10),A11(11),A12(12),A13(13),A14(14),A15(15),A16(16),A17(17),A18(18),A19(19)
    ,A20(20),A21(21),A22(22),A23(23),A24(24),A25(25),A26(26),A27(27),A28(28),A29(29)
    ,A30(30),A31(31),A32(32),A33(33),A34(34),A35(35),A36(36),A37(37),A38(38),A39(39)
    ,A40(40),A41(41),A42(42),A43(43),A44(44),A45(45),A46(46),A47(47),A48(48),A49(49)
    ,A50(50),A51(51),A52(52),A53(53),A54(54),A55(55),A56(56),A57(57),A58(58),A59(59)
    ,A60(60),A61(61),A62(62),A63(63),A64(64),A65(65),A66(66),A67(67),A68(68),A69(69)
    ,A70(70),A71(71),A72(72),A73(73),A74(74),A75(75),A76(76),A77(77),A78(78),A79(79)
    ,A80(80),A81(81),A82(82),A83(83),A84(84),A85(85),A86(86),A87(87),A88(88),A89(89)
    ,A90(90),A91(91),A92(92),A93(93),A94(94),A95(95),A96(96),A97(97),A98(98),A99(99)
  )

  def writing =
    IterablePipe(doubleCusts)
      .writeExecution(ParquetScroogeSource[Customers]("doubleCusts"))

  def writeU = {
    executesOk(writing)
    facts(
      "doubleCusts" </> "_SUCCESS"   ==> exists,
      "doubleCusts" </> "*.parquet"  ==> records(ParquetThermometerRecordReader[Customers], doubleCusts)
    )
  }

  def destruct(c: Customers): (String, String, String, Int) = (c: @unchecked) match {
    case OldCustomer(c) => (c.id, c.name, c.address, c.age)
    case NewCustomer(c) => (c.identifier.toString, c.name, c.address,  0 )
  }

  def reading =
    ParquetScroogeSource[Customers]("doubleCusts")
      .map( destruct(_) )
      .writeExecution(TypedPsv("doubleCusts.psv"))

  def readU = {
    executesOk(writing.flatMap(_ => reading))

    facts(
      "doubleCusts.psv" </> "_SUCCESS"   ==> exists,
      "doubleCusts.psv" </> "part-*"     ==> lines(data.flatMap(customer =>
        List( List(customer.id, customer.name, customer.address, customer.age).mkString("|"),
              List(customer.id.stripPrefix("CUSTOMER-"), customer.name, customer.address, 0).mkString("|"))))
    )
  }

  def writeReadNested = {

    def writeNested =
      IterablePipe(structCusts)
        .writeExecution(ParquetScroogeSource[UnionStruct]("structCusts"))

    def readNested =
      ParquetScroogeSource[UnionStruct]("structCusts")
        .collect( {case UnionStruct(AOrB.A, Some(NestedA(c))) => destruct(c) })
        .writeExecution(TypedPsv("doubleAgain.psv"))

    executesOk(writeNested.flatMap(_ => readNested))
    facts(
      "structCusts" </> "_SUCCESS"   ==> exists,
      "structCusts" </> "*.parquet"  ==> records(ParquetThermometerRecordReader[UnionStruct], structCusts),
      "doubleAgain.psv" </> "part-*" ==> lines(data.flatMap(customer =>
        List( List(customer.id, customer.name, customer.address, customer.age).mkString("|"),
              List(customer.id.stripPrefix("CUSTOMER-"), customer.name, customer.address, 0).mkString("|")))
      )
    )
  }


  def doWrite =
    IterablePipe(wideData)
      .writeExecution(ParquetScroogeSource[WideUnion]("wideData"))

  def writeWide = {
    executesOk(doWrite)

    facts(
      "wideData" </> "_SUCCESS"   ==> exists,
      "wideData" </> "*.parquet"  ==> records(ParquetThermometerRecordReader[WideUnion], wideData)
      )
  }

}
