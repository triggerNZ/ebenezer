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

import org.apache.hadoop.fs.Path

import org.apache.hadoop.hive.conf.HiveConf

import com.twitter.scrooge.ThriftStruct

import com.twitter.scalding._, TDsl._
import com.twitter.scalding.typed.IterablePipe

import scalaz.Scalaz._

import au.com.cba.omnia.thermometer.core.Thermometer._ // toPath

import au.com.cba.omnia.ebenezer.scrooge.hive.{Hive, HiveParquetScroogeSource}

/**
  * Building block methods for creating compatibility tests
  *
  * A compatibility test will typically take a paramater specifying where
  * to find files with older parquet formats, and perform a series of these
  * methods on those files to test our current code works on older formats.
  */
object Compat {
  def mrWrite[A <: ThriftStruct : Manifest](
    data: List[A], db: String, dstTable: String, path: Option[String] = None
  ): Execution[Unit] =
    IterablePipe(data)
      .writeExecution(HiveParquetScroogeSource[A](db, dstTable, path))

  def mrRead[A <: ThriftStruct : Manifest, B : Manifest : TupleConverter : TupleSetter](
    toTuple: A => B, db: String, srcTable: String, dst: String
  ): Execution[Unit] =
    HiveParquetScroogeSource[A](db, srcTable)
      .map(toTuple)
      .writeExecution(TypedPsv(dst))

  def hiveWrite[A <: ThriftStruct : Manifest](
    data: List[A], db: String, dstTable: String, tmpTable: String
  ): Execution[Unit] =
    IterablePipe(data)
      .writeExecution(HiveParquetScroogeSource[A](db, tmpTable))
      .flatMap(_ => Execution.from {
        Hive.createParquetTable[A](db, dstTable, List())
          .flatMap(_ => Hive.query(s"INSERT OVERWRITE TABLE $db.$dstTable SELECT * FROM $db.$tmpTable"))
          .run(new HiveConf)
      }).unit

  def hiveRead[A <: ThriftStruct : Manifest](
    db: String, srcTable: String, dstTable: String
  ): Execution[Unit] =
    Execution.from {
      Hive.createParquetTable[A](db, dstTable, List())
        .flatMap(_ => Hive.query(s"INSERT OVERWRITE TABLE $db.$dstTable SELECT * FROM $db.$srcTable"))
        .run(new HiveConf)
    }.unit

  def hiveSchema[A <: ThriftStruct : Manifest](
    db: String, table: String, path: Option[String] = None
  ): Execution[Unit] =
    Execution.from(Hive.createParquetTable[A](db, table, List(), path.map(_.toPath)).run(new HiveConf)).unit
}
