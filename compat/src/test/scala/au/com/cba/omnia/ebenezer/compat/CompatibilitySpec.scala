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

import au.com.cba.omnia.thermometer.core.Thermometer._
import au.com.cba.omnia.thermometer.fact.PathFactoids._
import au.com.cba.omnia.thermometer.hive.ThermometerHiveSpec

import au.com.cba.omnia.ebenezer.test.ParquetThermometerRecordReader

object CompatibilitySpec extends ThermometerHiveSpec { def is = s2"""
Internal compatibility tests for non collection types
=====================================================

  Can write using MR and read using MR     ${internal.readMRWriteMR}
  Can write using MR and read using Hive   ${internal.writeMRReadHive}
  Can write using Hive and read using MR   ${internal.writeHiveReadMR}
  Can write using Hive and read using Hive ${internal.writeHiveReadHive}

Backwards compatibility  tests
==============================

  parquet-1.5.0-cdh5.2.4
  ----------------------

    Can read simple MR data using MR     ${mRReadMR("parquet-1.5.0-cdh5.2.4")}
    Can read simple Hive data using MR   ${hiveReadMR("parquet-1.5.0-cdh5.2.4")}
    Can read simple MR data using hive   ${mRReadHive("parquet-1.5.0-cdh5.2.4")}
    Can read simple Hive data using hive ${hiveReadHive("parquet-1.5.0-cdh5.2.4")}

  parquet-1.2.5-cdh4.6.0
  ----------------------

    Can read simple MR data using MR     ${mRReadMR("parquet-1.2.5-cdh4.6.0")}
    Can read simple Hive data using MR   ${hiveReadMR("parquet-1.2.5-cdh4.6.0")}
    Can read simple MR data using hive   ${mRReadHive("parquet-1.2.5-cdh4.6.0")}
    Can read simple Hive data using hive ${hiveReadHive("parquet-1.2.5-cdh4.6.0")}

  parquet-1.2.5-cdh4.6.0-p337
  ---------------------------

    Can read simple MR data using MR     ${mRReadMR("parquet-1.2.5-cdh4.6.0-p337")}
    Can read simple Hive data using MR   ${hiveReadMR("parquet-1.2.5-cdh4.6.0-p337")}
    Can read simple MR data using hive   ${mRReadHive("parquet-1.2.5-cdh4.6.0-p337")}
    Can read simple Hive data using hive ${hiveReadHive("parquet-1.2.5-cdh4.6.0-p337")}

  parquet-1.2.5-cdh4.6.0-p485
  ---------------------------

    Can read simple MR data using MR     ${mRReadMR("parquet-1.2.5-cdh4.6.0-p485")}
    Can read simple Hive data using MR   ${hiveReadMR("parquet-1.2.5-cdh4.6.0-p485")}
    Can read simple MR data using hive   ${mRReadHive("parquet-1.2.5-cdh4.6.0-p485")}
    Can read simple Hive data using hive ${hiveReadHive("parquet-1.2.5-cdh4.6.0-p485")}


"""

  def writeMR(db: String, dst: String) = {
    val exec = Compat.mrWrite(Data.simple, db, dst)
    val fact = hiveWarehouse </> s"$db.db" </> dst </> "*.parquet" ==> records(ParquetThermometerRecordReader[Simple], Data.simple)
    executesOk(exec)
    facts(fact)
  }

  def readMR(db: String, src: String, dst: String) = {
    val exec = Compat.mrRead(
      (t: Simple) => (t._1, t._2, t._3, t._4, t._5, t._6, t._7, t._8, t._9),
      db, src, dst
    )
    val fact = dst </> "part-*" ==> lines(Data.simpleTsv)
    executesOk(exec)
    facts(fact)
  }

  def writeHive(db: String, tmpTable: String, dst: String) = {
    val exec = Compat.hiveWrite(Data.simple, db, dst, tmpTable)
    val fact = hiveWarehouse </> s"$db.db" </> dst </> "*" ==> records(ParquetThermometerRecordReader[Simple], Data.simple)
    executesOk(exec)
    facts(fact)
  }

  def readHive(db: String, src: String, dst: String) = {
    val exec = Compat.hiveRead[Simple](db, src, dst)
    val fact = hiveWarehouse </> s"$db.db" </> dst </> "*" ==> records(ParquetThermometerRecordReader[Simple], Data.simple)
    executesOk(exec)
    facts(fact)
  }

  def createExternalTable(db: String, src: String, path: String) = {
    val exec = Compat.hiveSchema[Simple](db, src, Some(path))
    executesOk(exec)
  }

  object internal {
    def readMRWriteMR = {
      val db  = "test"
      val src = "mrsrc"
      val dst = "mrdst"

      writeMR(db, src)
      readMR(db, src, dst)
    }

    def writeMRReadHive = {
      val db  = "test"
      val src = "mrsrc"
      val dst = "hivedst"

      writeMR(db, src)
      readHive(db, src, dst)
    }

    def writeHiveReadMR = {
      val db       = "test"
      val tmpTable = "staging"
      val src      = "hivesrc"
      val dst      = "mrdst"

      writeHive(db, tmpTable, src)
      readMR(db, src, dst)
    }

    def writeHiveReadHive = {
      val db       = "test"
      val tmpTable = "staging"
      val src      = "hivesrc"
      val dst      = "hivedst"

      writeHive(db, tmpTable, src)
      readHive(db, src, dst)
    }
  }

  def mRReadMR(parquetDir: String) =
    withEnvironment(path(getClass.getResource(s"/$parquetDir/simple/").toString)) {
      val db   = "test"
      val src  = "mrsrc"
      val path = s"$dir/user/ebenezer"
      val dst  = "mrdst"

      createExternalTable(db, src, path)
      readMR(db, src, dst)
    }

  def hiveReadMR(parquetDir: String) =
    withEnvironment(path(getClass.getResource(s"/$parquetDir/simple/").toString)) {
      val db   = "test"
      val src  = "hivesrc"
      val path = s"$dir/user/hive"
      val dst  = "mrdst"

      createExternalTable(db, src, path)
      readMR(db, src, dst)
    }

  def mRReadHive(parquetDir: String) =
    withEnvironment(path(getClass.getResource(s"/$parquetDir/simple/").toString)) {
      val db   = "test"
      val src  = "mrsrc"
      val path = s"$dir/user/ebenezer"
      val dst  = "hivedst"

      createExternalTable(db, src, path)
      readHive(db, src, dst)
    }

  def hiveReadHive(parquetDir: String) =
    withEnvironment(path(getClass.getResource(s"/$parquetDir/simple/").toString)) {
      val db   = "test"
      val src  = "hivesrc"
      val path = s"$dir/user/hive"
      val dst  = "hivedst"

      createExternalTable(db, src, path)
      readHive(db, src, dst)
    }
}
