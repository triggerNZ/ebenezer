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

import collection.JavaConverters._

import com.twitter.scalding.{Source, Args, Read, Write}

import cascading.pipe.Pipe
import cascading.tap.Tap

import cascading.flow.hive.HiveFlow

import au.com.cba.omnia.ebenezer.scrooge.scalding.UniqueJob

class HiveJob(args: Args, name: String, query: String, inputs: List[Source], output: Source)
    extends UniqueJob(args) {
  // Call the read method on each tap in order to add that tap to the flowDef.
  inputs.foreach(_.read(flowDef, mode))

  override def buildFlow = {
    new HiveFlow(name, query
      , inputs.map(_.createTap(Read)(mode).asInstanceOf[Tap[_, _, _]]).asJava
      , output.createTap(Write)(mode))
  }
}

object HiveJob {
  def apply(args: Args, name: String, query: String, inputs: List[Source], output: Source) =
    new HiveJob(args, name, query, inputs, output)

  def apply(args: Args, name: String, query: String, input: Source, output: Source) =
    new HiveJob(args, name, query, List(input), output)

}
