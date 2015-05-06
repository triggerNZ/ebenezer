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

import com.twitter.scalding.{Execution, ExecutionApp}
import com.twitter.scalding.typed.IterablePipe

import au.com.cba.omnia.ebenezer.ParquetLogging
import au.com.cba.omnia.ebenezer.scrooge.hive.PartitionHiveParquetScroogeSink


object HiveExampleStep1 extends ExecutionApp with ParquetLogging {
  val data = List(
    Customer("CUSTOMER-A", "Fred", "Bedrock", 40),
    Customer("CUSTOMER-2", "Wilma", "Bedrock", 40),
    Customer("CUSTOMER-3", "Barney", "Bedrock", 39),
    Customer("CUSTOMER-4", "BamBam", "Bedrock", 2)
  )

  def job = for {
    args <- Execution.getConfig.map(_.getArgs)
    _    <- execute(args("db"), args("table"), args.optional("location"))
  } yield ()

  def execute(db: String, table: String, location: Option[String] = None): Execution[Unit] =
    IterablePipe(data)
      .map(c => c.id -> c)
      .writeExecution(PartitionHiveParquetScroogeSink[String, Customer](db, table, List("pid" -> "string"), location))
}
