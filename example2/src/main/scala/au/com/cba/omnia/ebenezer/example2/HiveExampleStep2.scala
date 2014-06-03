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
    val job = ew HiveJob(args, "example", lmode, lflow,
      "INSERT OVERWRITE TABLE customers2 PARTITION (id) SELECT id,name,address,age FROM customers",
       inputs, output)

    val result = Jobs.runJob(job)
    println("Job Result:")
    println(result)
  }
}
