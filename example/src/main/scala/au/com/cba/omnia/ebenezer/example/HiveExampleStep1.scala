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
import org.apache.hadoop.hive.conf.HiveConf

import com.twitter.scalding.{Execution, ExecutionApp}
import com.twitter.scalding.typed.IterablePipe

import au.com.cba.omnia.beeswax.Hive

import au.com.cba.omnia.ebenezer.ParquetLogging
import au.com.cba.omnia.ebenezer.scrooge.PartitionParquetScroogeSink

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

  def execute(db: String, table: String, location: Option[String] = None): Execution[Unit] = {
    val conf = new HiveConf

    Execution.from({
      for {
        _    <- Hive.createParquetTable[Customer](db, table, List("pid" -> "string"), location.map(new Path(_)))
        path <- Hive.getPath(db, table)
      } yield path
    }.run(conf).toOption.get).flatMap { p =>
      IterablePipe(data).map(c => c.id -> c)
        .writeExecution(PartitionParquetScroogeSink[String, Customer]("pid=%s", p.toString))
    }.flatMap(_ => Execution.from(Hive.repair(db, table).run(conf))).map(_ => ()) 
  }
}
