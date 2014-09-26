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

package au.com.cba.omnia.ebenezer.compat.simple

import scala.collection.mutable.ArrayBuffer

import com.twitter.scalding._, TDsl._
import com.twitter.scalding.typed.IterablePipe

import au.com.cba.omnia.ebenezer.scrooge.hive.{HiveParquetScroogeSource, HiveJob}
import au.com.cba.omnia.ebenezer.compat.Simple

object Compatibility {
  val data = List(
    Simple("", true, 0, 0, 0, 0.0, 0, Some(""), Some(0.0)),
    Simple("a", true, -1, -1, -1, -1.0, 1, Some("string a"), Some(4.0)),
    Simple("a b", false, 50, 10, 20, 30.0, 5, Some("string a"), Some(10.0)),
    Simple("a b c", false, Short.MaxValue, Int.MaxValue, Long.MaxValue, Double.MaxValue, Byte.MaxValue, Some("string a"), Some(Double.MaxValue)),
    Simple("a b c", false, Short.MinValue, Int.MinValue, Long.MinValue, Double.MinValue, Byte.MinValue, Some("string a"), Some(Double.MinValue))
  )

  val dataTupled = data.map(t => (t._1, t._2, t._3, t._4, t._5, t._6, t._7, t._8, t._9))
  val dataTsv    = data.map(_.productIterator.toList.mkString("|"))

  def schema(db: String, table: String) = s"""
    CREATE TABLE $db.$table (
      stringfield string, booleanfield boolean, shortfield smallint, intfield int, longfield bigint,
      doublefield double, bytefield tinyint, optstringfield string, optdoublefield double
    ) ROW FORMAT SERDE 'parquet.hive.serde.ParquetHiveSerDe'
        STORED AS
          INPUTFORMAT "parquet.hive.DeprecatedParquetInputFormat"
          OUTPUTFORMAT "parquet.hive.DeprecatedParquetOutputFormat"
    """
}

class MRWriteJob(args: Args) extends Job(args) {
  val location = args.optional("location")

  IterablePipe(Compatibility.data, flowDef, mode)
    .write(HiveParquetScroogeSource[Simple](args("db"), args("dst"), location))
}

class MRReadJob(args: Args) extends Job(args) {
  HiveParquetScroogeSource[Simple](args("db"), args("src"))
    .map(t => (t._1, t._2, t._3, t._4, t._5, t._6, t._7, t._8, t._9))
    .write(TypedPsv(args("dst")))
}

class HiveReadJob(args: Args) extends CascadeJob(args) {
  val db       = args("db")
  val src      = args("src")
  val dst      = args("dst")

  val table = HiveParquetScroogeSource[Simple](db, dst)

  val jobs = List(HiveJob(
    args, "HiveReadJob",
    List(table), None,
    Compatibility.schema(db, dst),
    s"INSERT OVERWRITE TABLE $db.$dst SELECT * FROM $db.$src"
  ))
}

class HiveWriteJob(args: Args) extends CascadeJob(args) {
  val db      = args("db")
  val src     = args("tmpTable")
  val dst     = args("dst")

  val srcTable = HiveParquetScroogeSource[Simple](db, src)
  val table    = HiveParquetScroogeSource[Simple](db, dst)

  val location = args.optional("location")
  val jobs = List(
    new Job(args) {
      IterablePipe(Compatibility.data, flowDef, mode)
        .write(srcTable)
    },
    HiveJob(
      args, "HiveWriteJob",
      srcTable, Some(table),
      s"INSERT OVERWRITE TABLE $db.$dst SELECT * FROM $db.$src"
    )
  )
}
