package au.com.cba.omnia.ebenezer.example

import com.twitter.scalding._, TDsl._
import com.twitter.scalding.typed.IterablePipe

import org.apache.hadoop.hive.conf.HiveConf.ConfVars._

import au.com.cba.omnia.ebenezer.scrooge.hive._

class HiveExampleStep3(args: Args) extends CascadeJob(args) {
  val db       = args("db")
  val srcTable = args("src-table")
  val dstTable = args("dst-table")

  val intermediateOut = PartitionHiveParquetScroogeSink[String, Customer](db, srcTable, List("pid" -> "string"))
  val intermediateIn  = PartitionHiveParquetScroogeSource[Customer](db, srcTable, List("pid" -> "string"))
  val output          = PartitionHiveParquetScroogeSink[String, Customer](db, dstTable, List("pid" -> "string"))

  val data = List(
    Customer("CUSTOMER-A", "Fred", "Bedrock", 40),
    Customer("CUSTOMER-2", "Wilma", "Bedrock", 40),
    Customer("CUSTOMER-3", "Barney", "Bedrock", 39),
    Customer("CUSTOMER-4", "BamBam", "Bedrock", 2)
  )

  val jobs = List(
    new Job(args) {
      IterablePipe(data)
        .map(c => (c.id, c))
        .write(intermediateOut)
    },
    HiveJob(
      args, "example",
      intermediateIn, Some(output),
      Map(HIVEMERGEMAPFILES -> "true"),
      s"INSERT OVERWRITE TABLE $db.$dstTable PARTITION (pid) SELECT id, name, address, age, id as pid FROM $db.$srcTable",
      "CREATE TABLE test (id string, age int)",
      s"INSERT OVERWRITE TABLE TEST SELECT name, age from $db.$srcTable"
    )
  )
}
