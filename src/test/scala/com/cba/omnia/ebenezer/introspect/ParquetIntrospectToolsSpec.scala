package com.cba.omnia.ebenezer
package introspect

import cascading.flow.FlowDef
import cascading.tuple.Fields
import com.twitter.scalding._
import com.twitter.scalding.TDsl._
import com.twitter.scrooge._

import com.cba.omnia.ebenezer.test._, ThriftArbitraries._
import com.cba.omnia.ebenezer.scrooge._
import com.cba.omnia.thermometer.core._, Thermometer._
import com.cba.omnia.thermometer.tools._
import com.cba.omnia.thermometer.fact._, PathFactoids._

import java.nio.ByteBuffer

import org.apache.thrift.protocol.TCompactProtocol
import org.apache.thrift.transport.TIOStreamTransport

import parquet.thrift.ThriftSchemaConverter
import parquet.format.Util
import parquet.hadoop.ParquetReader

object ParquetIntrospectToolsSpec extends ThermometerSpec { def is = s2"""

Introspect Usage
================

  Read arbitrary parquet                    $read

Introspect Types
================

  Read booleans                             $boolish
  Read doubles                              $doublish
  Read bytes                                $bytish
  Read shorts                               $shortish
  Read ints                                 $intish
  Read longs                                $longish
  Read strings                              $stringish
  Read structs                              $nestedish
  Read lists                                $listish
  Read maps                                 $mapish
  Read emums                                $enumish

"""
  val data = List(
    Customer("CUSTOMER-1", "Fred", "Bedrock", 40),
    Customer("CUSTOMER-2", "Wilma", "Bedrock", 40),
    Customer("CUSTOMER-3", "Barney", "Bedrock", 39),
    Customer("CUSTOMER-4", "BamBam", "Bedrock", 2)
  )

  def read =
    typed("introspect", data)(customer => Record(List(
      Field("id", StringValue(customer.id)),
      Field("name", StringValue(customer.name)),
      Field("address", StringValue(customer.address)),
      Field("age", IntValue(customer.age))
    )))

  def boolish =
    typed("boolish", List(Boolish(true)))(d => Record(List(
      Field("value", BooleanValue(d.value))
    )))

  def doublish =
    typed("doublish", List(Doublish(1.1d)))(d => Record(List(
      Field("value", DoubleValue(d.value))
    )))

  def bytish =
    typed("bytish", List(Bytish(0x01.toByte)))(b => Record(List(
      Field("value", IntValue(b.value.toInt))
    )))

  def shortish =
    typed("shortish", List(Shortish(0x01.toShort)))(s => Record(List(
      Field("value", IntValue(s.value.toInt))
    )))

  def intish =
    typed("intish", List(Intish(0x01)))(i => Record(List(
      Field("value", IntValue(i.value))
    )))

  def longish =
    typed("longish", List(Longish(0x01l)))(l => Record(List(
      Field("value", LongValue(l.value))
    )))

  def stringish =
    typed("stringish", List(Stringish("yolo")))(s => Record(List(
      Field("value", StringValue(s.value))
    )))

  def nestedish =
    typed("nestedish", List(Nestedish(Nested("yolo"))))(s => Record(List(
      Field("value", RecordValue(
        Record(List(
          Field("value", StringValue(s.value.value))
        ))
      ))
    )))

  def listish =
    typed("listish", List(Listish(List("hello", "world"))))(r => Record(List(
      Field("values", ListValue(r.values.toList.map(StringValue)))
    )))

  def mapish =
    typed("mapish", List(Mapish(Map("yo" -> "lo", "lo" -> "yo"))))(r => Record(List(
      Field("values", MapValue(Map(r.values.toList.map({
        case (k, v) => StringValue(k) -> StringValue(v)
      }):_*)))
    )))

  def enumish = prop((data: List[Enumish]) =>
    typed("enumish", data)(r => Record(List(
      Field("value", EnumValue(r.value.name.toUpperCase))
    )))).set(minTestsOk = 1, workers = 1)

  def typed[A <: ThriftStruct](name: String, data: List[A])(expected: A => Record)(implicit M: Manifest[A]) =
    ThermometerSource(data)
      .write(ParquetScroogeSource[A](name))
      .withExpectations(context => {
        val result = context.glob(name </> "*.parquet").flatMap(
          ParquetIntrospectTools.listFromPath(conf, _))
        result.toSet must_== data.map(expected).toSet
      })
}
