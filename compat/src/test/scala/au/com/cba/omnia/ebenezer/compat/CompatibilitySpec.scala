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

package au.com.cba.omnia.ebenezer.compat

import com.twitter.scalding._

import au.com.cba.omnia.thermometer.core._, Thermometer._
import au.com.cba.omnia.thermometer.fact.PathFactoids._
import au.com.cba.omnia.thermometer.hive.HiveSupport

import au.com.cba.omnia.ebenezer.test.ParquetThermometerRecordReader

object CompatibilitySpec extends ThermometerSpec with HiveSupport { def is = sequential ^ s2"""
Internal compatibility tests for non collection types
=====================================================

  Can write using MR and read using MR     $readMRWriteMR
  Can write using MR and read using Hive   $writeMRReadHive
  Can write using Hive and read using MR   $writeHiveReadMR
  Can write using Hive and read using Hive $writeHiveReadHive
"""

  def writeMR(dst: String) = {
    val job = withArgs(Map("db" -> "default", "dst" -> dst))(new simple.MRWriteJob(_))
    val fact = hiveWarehouse </> dst </> "*.parquet" ==> records(ParquetThermometerRecordReader[Simple], simple.Compatibility.data)

    job.withFacts(fact)
  }

  def readMR(src: String, dst: String) = {
    val job = withArgs(Map("db" -> "default", "src" -> src, "dst" -> dst))(new simple.MRReadJob(_))
    val fact = dst </> "part-*" ==> lines(simple.Compatibility.dataTsv)

    job.withFacts(fact)
  }

  def writeHive(tmpTable: String, dst: String) = {
    val job = withArgs(Map("db" -> "default", "tmpTable" -> tmpTable, "dst" -> dst))(new simple.HiveWriteJob(_))
    val fact = hiveWarehouse </> dst </> "*" ==> records(ParquetThermometerRecordReader[Simple], simple.Compatibility.data)

    job.withFacts(fact)
  }

  def readHive(src: String, dst: String) = {
    val job = withArgs(Map("db" -> "default", "src" -> src, "dst" -> dst))(new simple.HiveReadJob(_))
    val fact = hiveWarehouse </> dst </> "*" ==> records(ParquetThermometerRecordReader[Simple], simple.Compatibility.data)

    job.withFacts(fact)
  }

  def readMRWriteMR = {
    val src = "mrsrc"
    val dst = "mrdst"

    withDependency(writeMR(src))(readMR(src, dst))
  }

  def writeMRReadHive = {
    val src      = "mrsrc"
    val dst      = "hivedst"

    withDependency(writeMR(src))(readHive(src, dst))
  }

  def writeHiveReadMR = {
    val tmpTable = "staging"
    val src      = "hivesrc"
    val dst      = "mrdst"

    withDependency(writeHive(tmpTable, src))(readMR(src, dst))
  }

  def writeHiveReadHive = {
    val tmpTable = "staging"
    val src      = "hivesrc"
    val dst      = "hivedst"

    withDependency(writeHive(tmpTable, src))(readHive(src, dst))
  }
}
