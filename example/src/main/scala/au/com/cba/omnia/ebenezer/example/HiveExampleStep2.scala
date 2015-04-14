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

import com.twitter.scalding._, TDsl._
import com.twitter.scalding.typed.IterablePipe

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.conf.HiveConf.ConfVars._

import au.com.cba.omnia.ebenezer.ParquetLogging
import au.com.cba.omnia.ebenezer.scrooge.hive._

object HiveExampleStep2 {
  val data = List(
    Customer("CUSTOMER-A", "Fred", "Bedrock", 40),
    Customer("CUSTOMER-2", "Wilma", "Bedrock", 40),
    Customer("CUSTOMER-3", "Barney", "Bedrock", 39),
    Customer("CUSTOMER-4", "BamBam", "Bedrock", 2)
  )

  def execute(db: String, srcTable: String, dstTable: String) = {
    val intermediateOut = PartitionHiveParquetScroogeSink[String, Customer](db, srcTable, List("pid" -> "string"))
    val intermediateIn  = PartitionHiveParquetScroogeSource[Customer](db, srcTable, List("pid" -> "string"))
    val output          = PartitionHiveParquetScroogeSink[String, Customer](db, dstTable, List("pid" -> "string"))

    val conf            = new HiveConf
    conf.setVar(HIVEMERGEMAPFILES, "true")

    IterablePipe(data)
      .map(c => (c.id, c))
      .writeExecution(intermediateOut)
      .flatMap(_ => Execution.from(
        Hive.createParquetTable[Customer](db, dstTable, List("pid" -> "string"))
          .flatMap(_ => Hive.queries(List(
            s"INSERT OVERWRITE TABLE $db.$dstTable PARTITION (pid) SELECT id, name, address, age, id as pid FROM $db.$srcTable",
            "CREATE TABLE test (id string, age int)",
            s"INSERT OVERWRITE TABLE TEST SELECT name, age from $db.$srcTable"
          )))
          .run(conf)
      ))
  }
}
