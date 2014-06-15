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

package au.com.cba.omnia.ebenezer.scrooge
package hive

import scalding._
import com.twitter.scalding._
import org.apache.hadoop.hive.conf.HiveConf
import cascading.scheme.Scheme
import cascading.flow.FlowDef
import cascading.flow.hive.HiveFlow
import cascading.tap.hive.{HiveTap, HiveTableDescriptor}
import cascading.tap.{Tap, SinkMode}
import au.com.cba.omnia.ebenezer.scrooge.ParquetScroogeScheme
import org.apache.hadoop.mapred.{OutputCollector, RecordReader, JobConf}

class HiveJob(args: Args, name : String, query: String, lflow: FlowDef, lmode: Mode, inputTaps: List[Source], outputTap: Source) extends UniqueJob(args) {
  override val flowDef: FlowDef = lflow
  override def mode: Mode = lmode

  import collection.JavaConversions._

  override def buildFlow = {
    new HiveFlow(name, query
      , inputTaps.map(_.createTap(Read)(mode).asInstanceOf[Tap[_, _, _]])
      , outputTap.createTap(Write)(mode))
  }

}
