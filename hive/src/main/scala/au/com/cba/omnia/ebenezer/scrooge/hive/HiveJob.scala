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
import cascading.flow.{Flow, FlowSkipStrategy}

import cascading.tap.hive.HiveNullTap
import cascading.flow.hive.HiveFlow

import au.com.cba.omnia.ebenezer.scrooge.scalding.UniqueJob

/**
  * Creates a Scalding job to run the specified queries against hive.
  * 
  * The specified inputs and output are not directly used as part of the query. Instead they are
  * used by Cascade to determine how to schedule this job in relation to other jobs.
  */
class HiveJob(args: Args, name: String, inputs: List[Source], output: Option[Source], queries: Seq[String])
    extends UniqueJob(args) {
  // Call the read method on each tap in order to add that tap to the flowDef.
  inputs.foreach(_.read(flowDef, mode))

  override def buildFlow = {
    val flow = new HiveFlow(
      name, queries.toArray,
      inputs.map(_.createTap(Read)(mode).asInstanceOf[Tap[_, _, _]]).asJava,
      output.fold[Tap[_, _, _]](HiveNullTap.DEV_NULL)(_.createTap(Write)(mode))
    )

    flow.setFlowSkipStrategy(DontSkipStrategy)
    flow
  }
}

object HiveJob {
  /**
    * Creates a Scalding job to run the specified queries against hive.
    * 
    * The specified inputs and output are not directly used as part of the query. Instead they are
    * used by Cascading to determine how to schedule this job in relation to other jobs.
    */
  def apply(args: Args, name: String, inputs: List[Source], output: Option[Source], query: String*) =
    new HiveJob(args, name, inputs, output, query)

  /**
    * Creates a Scalding job to run the specified queries against hive.
    * 
    * The specified input and output are not directly used as part of the query. Instead they are
    * used by Cascading to determine how to schedule this job in relation to other jobs.
    */
  def apply(args: Args, name: String, input: Source, output: Option[Source], query: String*) =
    new HiveJob(args, name, List(input), output, query)

}

/** Cascading flow skip strategy that does not skip a flow.*/
object DontSkipStrategy extends FlowSkipStrategy {
  def skipFlow(flow: Flow[_]): Boolean = false
}
