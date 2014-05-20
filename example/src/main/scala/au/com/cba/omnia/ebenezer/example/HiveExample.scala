package au.com.cba.omnia.ebenezer
package example

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

// class TestJob(args: Args) extends Job(args) {
//   val data = List(
//     Customer("CUSTOMER-A", "Fred", "Bedrock", 40),
//     Customer("CUSTOMER-2", "Wilma", "Bedrock", 40),
//     Customer("CUSTOMER-3", "Barney", "Bedrock", 39),
//     Customer("CUSTOMER-4", "BamBam", "Bedrock", 2)
//   )
// 
//   val conf = new HiveConf
// 
//   IterablePipe(data, flowDef, mode)
//     .map(c => c.id -> c)
//     .write(PartitionHiveParquetScroogeSink[String, Customer](args("db"), args("table"), List("id" -> "string"), conf))
// }

object TestHQLJob {
  def main(argsS: Array[String]) {
    import scala.collection.JavaConversions._
    val conf = new HiveConf()
    val args = Args(argsS)
    val lmode = ScaldingSupport.mode
    val lflow = ScaldingSupport.flow
    
    val inputSource = PartitionHiveParquetScroogeSource[Customer]("example", "customers", List("id" -> "string"), conf)
    FlowStateMap.mutate(lflow) { st =>
      val newPipe = new Pipe(inputSource.toString)
      st.getReadPipe(inputSource, newPipe)
    }
    
    val inputTaps = List(inputSource.createTap(Read)(lmode).asInstanceOf[Tap[_, _, _]])
    
    val outputTap =
      PartitionHiveParquetScroogeSink[String, Customer]("example", "customers2", List("id" -> "string"), conf)
      .createTap(Write)(lmode)
    
    val job = new HiveJob(args, "example",
      "SELECT * FROM customers",
       inputTaps, outputTap) {
      override val flowDef: FlowDef = lflow
      override def mode: Mode = lmode
    }

    
    // val hiveFlow = new HiveFlow("example", "SELECT * FROM customers", seqAsJavaList(inputTaps), outputTap)
    // new HiveFlow("hivego", "SELECT * FROM customers", Array(IterablePipe(data, flowDef, mode)).to,
    //   PartitionHiveParquetScroogeSink[String, Customer](args("db"), args("table"), List("id" -> "string"), conf))
    
    // val result = Flows.runFlow(args, hiveFlow, mode)
    val result = Jobs.runJob(job)
    println(result)
  }
}
