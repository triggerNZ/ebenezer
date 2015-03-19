package au.com.cba.omnia.ebenezer.example

import com.twitter.scalding._, TDsl._
import com.twitter.scalding.typed.IterablePipe

import org.apache.hadoop.hive.conf.HiveConf.ConfVars._

import au.com.cba.omnia.ebenezer.ParquetLogging
import au.com.cba.omnia.ebenezer.scrooge.hive._

object HiveExampleStep5 extends ExecutionApp with ParquetLogging {
  def job = null

  val data = List(
    Customer("CUSTOMER-A", "Fred", "Bedrock", 40),
    Customer("CUSTOMER-2", "Wilma", "Bedrock", 40),
    Customer("CUSTOMER-3", "Barney", "Bedrock", 39),
    Customer("CUSTOMER-4", "BamBam", "Bedrock", 2)
  )

  def execute(db: String, src: String, dst: String): Execution[Unit] = {
    val intermediateOut = PartitionHiveParquetScroogeSink[String, Customer](db, src, List("pid" -> "string"))
    val intermediateIn  = PartitionHiveParquetScroogeSource[Customer](db, src, List("pid" -> "string"))
    val output          = PartitionHiveParquetScroogeSink[String, Customer](db, dst, List("pid" -> "string"))


    IterablePipe(data)
      .map(c => (c.id, c))
      .writeExecution(intermediateOut)
      .flatMap(_ => HiveExecution.query(
        "example", output,
        Map(HIVEMERGEMAPFILES -> "true"),
        s"INSERT OVERWRITE TABLE $db.$dst PARTITION (pid) SELECT id, name, address, age, id as pid FROM $db.$src"
      ))
  }

  override def main(args: Array[String]): Unit = {
    config(args)  match {
      case (conf, mode) => {
        val args = conf.getArgs
        println(execute(args("db"), args("src-tabe"), args("dst-table")).waitFor(conf, mode))
      }
    }
  }
}
