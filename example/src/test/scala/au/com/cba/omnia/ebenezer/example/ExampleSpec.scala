//   Copyright 2014 Commonwealth Bank of Australia
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

package au.com.cba.omnia.ebenezer.example

import com.twitter.scalding._

import au.com.cba.omnia.thermometer.core._, Thermometer._
import au.com.cba.omnia.thermometer.fact.PathFactoids._
import au.com.cba.omnia.thermometer.hive.HiveSupport

import au.com.cba.omnia.ebenezer.test.ParquetThermometerRecordReader

object ExampleSpec extends ThermometerSpec with HiveSupport { def is = sequential ^ s2"""
  Examples
  ========

  Example 1 runs $example1
  Example 2 doesn't run since it depends on an existing table
  Example 3 runs $example3
  Example 4 runs $example4
  Example 5 runs $example5
"""

  def example1 = {
    val job = withArgs(Map("db" -> "default", "table" -> "dst"))(new HiveExampleStep1(_))
    val facts = job.data.groupBy(_.id).map { case (k, vs) => 
      hiveWarehouse </> "dst" </> s"pid=$k" </> "*.parquet" ==> records(ParquetThermometerRecordReader[Customer], vs)
    }.toList

    job.withFacts(facts: _*)
  }

  def example3 = {
    val job = withArgs(Map("db" -> "default", "src-table" -> "src", "dst-table" -> "dst"))(new HiveExampleStep3(_))
    val facts = (hiveWarehouse </> "test" </> "0*" ==> count(4)) +: job.data.flatMap(c => List(
      hiveWarehouse </> "src" </> s"pid=${c.id}" </> "*.parquet" ==> recordCount(ParquetThermometerRecordReader[Customer], 1),
      hiveWarehouse </> "dst" </> s"pid=${c.id}" </> "0*"        ==> recordCount(ParquetThermometerRecordReader[Customer], 1)
      )) 

    job.withFacts(facts: _*)
  }

  def example4 = {
    val job = withArgs(Map("db" -> "default", "table" -> "dst"))(new HiveExampleStep4(_))
    val fact =
      hiveWarehouse </> "dst" </> "*.parquet" ==> records(ParquetThermometerRecordReader[Nested], job.data)

    job.withFacts(fact)
  }

  def example5 = {
    executesOk(HiveExampleStep5.execute("test", "src", "dst"))
    facts(HiveExampleStep5.data.flatMap(c => List(
      hiveWarehouse </> "test.db" </> "src" </> s"pid=${c.id}" </> "*.parquet" ==> recordCount(ParquetThermometerRecordReader[Customer], 1),
      hiveWarehouse </> "test.db" </> "dst" </> s"pid=${c.id}" </> "0*"        ==> recordCount(ParquetThermometerRecordReader[Customer], 1)
    )): _*)
  }
}
