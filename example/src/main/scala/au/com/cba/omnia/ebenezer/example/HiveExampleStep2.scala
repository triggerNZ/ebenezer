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

import org.apache.hadoop.fs.Path

import com.twitter.scalding.Execution
import com.twitter.scalding.typed.IterablePipe

import org.apache.hadoop.hive.conf.HiveConf

import au.com.cba.omnia.beeswax.Hive

import au.com.cba.omnia.ebenezer.ParquetLogging
import au.com.cba.omnia.ebenezer.scrooge.PartitionParquetScroogeSink

object HiveExampleStep2 {
  val data = List(
    Customer("CUSTOMER-A", "Fred", "Bedrock", 40),
    Customer("CUSTOMER-2", "Wilma", "Bedrock", 40),
    Customer("CUSTOMER-3", "Barney", "Bedrock", 39),
    Customer("CUSTOMER-4", "BamBam", "Bedrock", 2)
  )

  def execute(db: String, srcTable: String, dstTable: String) = {
    val conf = new HiveConf

    Execution.from({
      for {
        _    <- Hive.createParquetTable[Customer](db, srcTable, List("pid" -> "string"))
        path <- Hive.getPath(db, srcTable)
      } yield path.toString
    }.run(conf).toOption.get).flatMap { p =>
      IterablePipe(data).map(c => c.id -> c)
        .writeExecution(PartitionParquetScroogeSink[String, Customer]("pid=%s", p))
    }.flatMap(_ => Execution.from(
      (for {
        path <- Hive.getPath(db, srcTable)
        _    <- Hive.addPartitions(db, srcTable, List("pid"), data.map(c => new Path(path, s"pid=${c.id}")))
        _    <- Hive.createParquetTable[Customer](db, dstTable, List("pid" -> "string"))
        _    <- Hive.queries(List(
                  s"INSERT OVERWRITE TABLE $db.$dstTable PARTITION (pid) SELECT id, name, address, age, id as pid FROM $db.$srcTable",
                  "CREATE TABLE test (id string, age int)",
                  s"INSERT OVERWRITE TABLE TEST SELECT name, age from $db.$srcTable"
                ))
      } yield ()).run(conf)
    ))
  }
}
