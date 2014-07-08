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

import au.com.cba.omnia.ebenezer.scrooge.hive.PartitionHiveParquetScroogeSink

class HiveExampleStep1(args: Args) extends Job(args) {
  val data = List(
    Customer("CUSTOMER-A", "Fred", "Bedrock", 40),
    Customer("CUSTOMER-2", "Wilma", "Bedrock", 40),
    Customer("CUSTOMER-3", "Barney", "Bedrock", 39),
    Customer("CUSTOMER-4", "BamBam", "Bedrock", 2)
  )

  val conf = new HiveConf

  IterablePipe(data, flowDef, mode)
    .map(c => c.id -> c)
    .write(PartitionHiveParquetScroogeSink[String, Customer](args("db"), args("table"), List("pid" -> "string"), conf))
}
