package au.com.cba.omnia.ebenezer
package example

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
    .write(PartitionHiveParquetScroogeSink[String, Customer](args("db"), args("table"), List("id" -> "string"), conf))
}
