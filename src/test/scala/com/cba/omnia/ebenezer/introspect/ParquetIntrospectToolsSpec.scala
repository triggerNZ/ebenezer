package com.cba.omnia.ebenezer
package introspect

import cascading.flow.FlowDef
import cascading.tuple.Fields
import com.twitter.scalding._
import com.twitter.scalding.TDsl._
import com.twitter.scrooge._

import com.cba.omnia.ebenezer.test._
import com.cba.omnia.ebenezer.scrooge._
import com.cba.omnia.thermometer.core._, Thermometer._
import com.cba.omnia.thermometer.tools._
import com.cba.omnia.thermometer.fact._, PathFactoids._

import org.apache.thrift.protocol.TCompactProtocol
import org.apache.thrift.transport.TIOStreamTransport

import parquet.thrift.ThriftSchemaConverter
import parquet.format.Util
import parquet.hadoop.ParquetReader

object ParquetIntrospectToolsSpec extends ThermometerSpec { def is = s2"""

Introspect Usage
================

  Read arbitrary parquet                    $read

"""
  val data = List(
    Customer("CUSTOMER-1", "Fred", "Bedrock", 40),
    Customer("CUSTOMER-2", "Wilma", "Bedrock", 40),
    Customer("CUSTOMER-3", "Barney", "Bedrock", 39),
    Customer("CUSTOMER-4", "BamBam", "Bedrock", 2)
  )

  def read =
    ThermometerSource(data)
      .write(ParquetScroogeSource[Customer]("introspect"))
      .withExpectations(context => {
        val result = context.glob("introspect" </> "*.parquet").flatMap(
          ParquetIntrospectTools.listFromPath(conf, _))
        result.toSet must_== data.map(customer =>
          Record(List(
            Field("id", StringValue(customer.id)),
            Field("name", StringValue(customer.name)),
            Field("address", StringValue(customer.address)),
            Field("age", IntValue(customer.age))
          ))
        ).toSet
      })
}
