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

import au.com.cba.omnia.thermometer.core.Thermometer._
import au.com.cba.omnia.thermometer.fact.PathFactoids._
import au.com.cba.omnia.thermometer.hive.ThermometerHiveSpec

import au.com.cba.omnia.ebenezer.ParquetLogging
import au.com.cba.omnia.ebenezer.test.ParquetThermometerRecordReader

object ExampleSpec extends ThermometerHiveSpec with ParquetLogging { def is = sequential ^ s2"""
  Examples
  ========

  Example 1 runs $example1
  Example 2 runs $example2
  Example 3 runs $example3
  Example 4 runs $example4
"""

  def example1 = {
    executesOk(HiveExampleStep1.execute("default", "dst"))
    facts(
      HiveExampleStep1.data.groupBy(_.id).map { case (k, vs) =>
        hiveWarehouse </> "dst" </> s"pid=$k" </> "*.parquet" ==> records(ParquetThermometerRecordReader[Customer], vs)
      }.toList: _*
    )
  }

  def example2 = {
    executesOk(HiveExampleStep2.execute("default", "src", "dst"))
    facts((hiveWarehouse </> "test" </> "0*" ==> count(4)) +: HiveExampleStep2.data.flatMap(c => List(
      hiveWarehouse </> "src" </> s"pid=${c.id}" </> "*.parquet" ==> recordCount(ParquetThermometerRecordReader[Customer], 1),
      hiveWarehouse </> "dst" </> s"pid=${c.id}" </> "0*"        ==> recordCount(ParquetThermometerRecordReader[Customer], 1)
    )): _*)
  }

  def example3 = {
    executesOk(HiveExampleStep3.execute("default", "dst"))
    facts(
      hiveWarehouse </> "dst" </> "*.parquet" ==> records(ParquetThermometerRecordReader[Nested], HiveExampleStep3.data)
    )
  }

  def example4 = {
    executesOk(HiveExampleStep4.execute("test", "src", "dst"))
    facts(HiveExampleStep4.data.flatMap(c => List(
      hiveWarehouse </> "test.db" </> "src" </> s"pid=${c.id}" </> "*.parquet" ==> recordCount(ParquetThermometerRecordReader[Customer], 1),
      hiveWarehouse </> "test.db" </> "dst" </> s"pid=${c.id}" </> "0*"        ==> recordCount(ParquetThermometerRecordReader[Customer], 1)
    )): _*)
  }
}
