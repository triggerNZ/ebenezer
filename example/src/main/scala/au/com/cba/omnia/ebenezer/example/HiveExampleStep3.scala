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

import scalaz.Scalaz._

import com.twitter.scalding.{Execution, ExecutionApp}
import com.twitter.scalding.typed.IterablePipe

import org.apache.hadoop.hive.conf.HiveConf

import au.com.cba.omnia.beeswax.Hive

import au.com.cba.omnia.ebenezer.ParquetLogging
import au.com.cba.omnia.ebenezer.scrooge.ParquetScroogeSource

object HiveExampleStep3 extends ExecutionApp with ParquetLogging {
  val data = List(
    Nested(Map(
      1 -> Map(10 -> List("a1", "b1")),
      2 -> Map(20 -> List("a2", "b2")),
      3 -> Map(30 -> List("a3", "b3"))
    )),
    Nested(Map(
      1 -> Map(10 -> List("q1", "r1")),
      2 -> Map(20 -> List("q2", "r2")),
      3 -> Map(30 -> List("q3", "r3"))
    )),
    Nested(Map(
      1 -> Map(10 -> List("x1", "z1")),
      2 -> Map(20 -> List("x2", "z2")),
      3 -> Map(30 -> List("x3", "z3"))
    ))
  )

  def job = for {
    args <- Execution.getConfig.map(_.getArgs)
    _    <- execute(args("db"), args("table"))
  } yield ()

  def execute(db: String, table: String) = {
    val conf = new HiveConf

    Execution.from({
      for {
        _    <- Hive.createParquetTable[Nested](db, table, List.empty)
        path <- Hive.getPath(db, table)
      } yield path.toString
    }.run(conf).toOption.get).flatMap { p =>
      IterablePipe(data)
        .writeExecution(ParquetScroogeSource[Nested](p))
    }
  }
}
