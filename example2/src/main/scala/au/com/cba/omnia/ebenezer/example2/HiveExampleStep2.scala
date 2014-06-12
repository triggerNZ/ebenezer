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

package au.com.cba.omnia.ebenezer
package example2

import example._
import scalaz._, Scalaz._
import scrooge.hive._
import com.twitter.scalding._
import cascading.flow.FlowDef
import cascading.flow.hive.HiveFlow
import cascading.tap.{Tap, SinkMode}
import org.apache.hadoop.hive.conf.HiveConf
import com.twitter.scalding.TDsl._
import com.twitter.scalding.typed.IterablePipe
import au.com.cba.omnia.thermometer.tools._
import cascading.pipe.Pipe

object HiveExampleStep2 {
  def main(argsS: Array[String]) {
    import scala.collection.JavaConversions._
    val conf = new HiveConf()
    val args = Args(argsS)
    val lmode = Hdfs(false, conf)
    val lflow = new FlowDef <| (_.setName("hql-example"))
    Mode.putMode(lmode, args)
    
    val inputs = List(PartitionHiveParquetScroogeSource[Customer]("default", "customers", List("id" -> "string"), conf, lflow))
    val output = PartitionHiveParquetScroogeSink[String, Customer]("default", "customers2", List("id" -> "string"), conf)
    val job = new HiveJob(args, "example",
      "INSERT OVERWRITE TABLE customers2 PARTITION (id) SELECT id,name,address,age FROM customers",
       lflow, lmode,
       inputs, output)

    val result = Jobs.runJob(job)
    println("Job Result:")
    println(result)
  }
}
