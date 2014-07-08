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

import org.apache.hadoop.hive.conf.HiveConf

import au.com.cba.omnia.ebenezer.scrooge.hive._

class HiveExampleStep2(args: Args) extends CascadeJob(args) {
  val conf = new HiveConf()
  val db = args("db")
  val srcTable = args("src-table")
  val dstTable = args("dst-table")

  val inputs = List(PartitionHiveParquetScroogeSource[Customer](db, srcTable, List("pid" -> "string"), conf))
  val output = PartitionHiveParquetScroogeSink[String, Customer](db, dstTable, List("pid" -> "string"), conf)

  val jobs = List(HiveJob(
    args, "example",
    s"INSERT OVERWRITE TABLE $db.$dstTable PARTITION (pid) SELECT id, name, address, age, id as pid FROM $db.$srcTable",
    inputs, Some(output)
  ))
}
