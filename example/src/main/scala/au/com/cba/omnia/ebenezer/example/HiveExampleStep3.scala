package au.com.cba.omnia.ebenezer.example

import com.twitter.scalding._, TDsl._
import com.twitter.scalding.typed.IterablePipe

import org.apache.hadoop.hive.conf.HiveConf

import au.com.cba.omnia.ebenezer.scrooge.hive._

class HiveExampleStep3(args: Args) extends CascadeJob(args) {
  val conf = new HiveConf()
  val db = args("db")
  val srcTable = args("src-table")
  val dstTable = args("dst-table")

  val intermediate = HiveParquetScroogeSource[Customer](db, srcTable, conf)
  val output       = PartitionHiveParquetScroogeSink[String, Customer](db, dstTable, List("pid" -> "string"), conf)

  val data = List(
    Customer("CUSTOMER-A", "Fred", "Bedrock", 40),
    Customer("CUSTOMER-2", "Wilma", "Bedrock", 40),
    Customer("CUSTOMER-3", "Barney", "Bedrock", 39),
    Customer("CUSTOMER-4", "BamBam", "Bedrock", 2)
  )

  val jobs = List(
    new Job(args) {
      IterablePipe(data, flowDef, mode)
        .write(intermediate)
    },
    HiveJob(
      args, "example",
      s"INSERT OVERWRITE TABLE $db.$dstTable PARTITION (pid) SELECT id, name, address, age, id as pid FROM $db.$srcTable",
      intermediate, Some(output)
    )
  )
}
