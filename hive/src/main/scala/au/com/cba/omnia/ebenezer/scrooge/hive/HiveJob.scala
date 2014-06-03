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
