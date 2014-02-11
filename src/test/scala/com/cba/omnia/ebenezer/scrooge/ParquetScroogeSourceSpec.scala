package com.cba.omnia.ebenezer
package scrooge

import cascading.flow.FlowDef
import cascading.tuple.Fields
import com.twitter.scalding._
import com.twitter.scalding.TDsl._
import com.twitter.scrooge._

import com.cba.omnia.ebenezer.test._
import com.cba.omnia.thermometer.core._, Thermometer._
import com.cba.omnia.thermometer.tools._
import com.cba.omnia.thermometer.fact._, PathFactoids._

import org.apache.hadoop.mapred.JobConf

import scalaz.effect.IO

object ParquetScroogeSourceSpec extends ThermometerSpec { def is = s2"""

ParquetSource Usage
===================

  Write to parquet                          $write

"""
  val data = List(
    Customer("CUSTOMER-1", "Fred", "Bedrock", 40),
    Customer("CUSTOMER-2", "Wilma", "Bedrock", 40),
    Customer("CUSTOMER-3", "Barney", "Bedrock", 39),
    Customer("CUSTOMER-4", "BamBam", "Bedrock", 2)
  )

  def write =
    ThermometerSource(data)
      .write(ParquetScroogeSource[Customer]("customers"))
      .withFacts(
        "customers" </> "_SUCCESS"   ==> exists
      , "customers" </> "*.parquet"  ==> records(format[Customer](conf), data)
      )


  def format[A <: ThriftStruct : Manifest](conf: JobConf): ThermometerFormat[IO, A] =
    ThermometerFormat(
      path         => IO { ParquetScroogeTools.listFromPath[A](conf, path) }
    , (path, data) => IO { () }
    )
}
