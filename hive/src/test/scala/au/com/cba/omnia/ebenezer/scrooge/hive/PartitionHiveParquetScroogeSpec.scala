package au.com.cba.omnia.ebenezer.scrooge
package hive

import scalaz.effect.IO

import com.twitter.scalding._, TDsl._
import com.twitter.scalding.typed.IterablePipe

import com.twitter.scrooge.ThriftStruct

import cascading.tap.Tap

import org.apache.hadoop.mapred.JobConf

import au.com.cba.omnia.thermometer.core._, Thermometer._
import au.com.cba.omnia.thermometer.hive.HiveSupport
import au.com.cba.omnia.thermometer.fact.PathFactoids._

class PartitionHiveParquetScroogeSpec extends ThermometerSpec with HiveSupport { def is = s2"""
PartitionHiveParquetScrooge Source and Sink Test
================================================

Hive support for (thrift scrooge encoded) partitioned parquet tables
  should allow for writing to a sink $writeTest
  and then it should be possible to read from the data that is written to the sink $readTest
  identifier test $identifierTest
  should allow for writing to a table already created by Hive $writeExistingTest

"""

  val partitionColumns = List("p_domain" -> "string", "p_partition" -> "bigint", "p_batch" -> "string")
  val database         = "database"
  val table            = "fake_records"

  val data = List(
    SimpleHive("aaa"), SimpleHive("bbb"), SimpleHive("ccc")
  )

  val data2 = data.map(TestHelper.change)

  def write = {
    IterablePipe(data)
      .map(rec => ("rdbms_changes", 0L, "test" ) -> rec)
      .writeExecution(PartitionHiveParquetScroogeSink[(String, Long, String), SimpleHive](
          database, table, partitionColumns
      ))
  }

  def writeTest = {
    executesOk(write)

    val path = hiveWarehouse </> "database.db" </> "fake_records"

    facts(
      path ==> exists,
      path</> s"p_domain=rdbms_changes" </> "*" </> "*" </> "*.parquet" ==> records(
          ParquetThermometerRecordReader[SimpleHive],
          data
        )
    )
  }

  def read = {
    PartitionHiveParquetScroogeSource[SimpleHive](database, table, partitionColumns)
      .map(x =>  ("table", 1L) -> TestHelper.change(x))
      .writeExecution(PartitionHiveParquetScroogeSink[(String, Long), SimpleHive2](
        database,
        "some_other_fake_records_table",
        List("p_entity" -> "string", "p_partition" -> "bigint")
      ))
  }

  def readTest = {
    executesOk(write.flatMap(_ => read))

    val path = hiveWarehouse </> "database.db" </> "some_other_fake_records_table"


    facts(
      path ==> exists,
      path </> s"p_entity=table" </> "p_partition=1" </> "*.parquet" ==> records(
          ParquetThermometerRecordReader[SimpleHive2],
          data2
        )
    )
  }

  def identifierTest = {
    val source = PartitionHiveParquetScroogeSource[SimpleHive](database, table, partitionColumns)
    val sink   = PartitionHiveParquetScroogeSink[(String, Long, String), SimpleHive](
      database, table, partitionColumns
    )


    val exec = Execution.getMode.map { mode =>
      val sourceTap    = source.createTap(Read)(mode).asInstanceOf[Tap[JobConf, _, _]]
      val sourceId     = sourceTap.getIdentifier
      val sourceFullId = sourceTap.getFullIdentifier(jobConf)

      val sinkTap    = sink.createTap(Write)(mode).asInstanceOf[Tap[JobConf, _, _]]
      val sinkId     = sinkTap.getIdentifier
      val sinkFullId = sinkTap.getFullIdentifier(jobConf)

      (sourceId, sourceFullId, sinkId, sinkFullId)
    }

    val (sourceId, sourceFullId, sinkId, sinkFullId) =  executesSuccessfully(exec)

    sourceId     must_== sinkId
    sourceFullId must_== sinkFullId
  }

  def writeExistingTest = {
    // DDL needs to match SimpleHive plus partition columns
    val partitionColumnsDdl =
      partitionColumns.map(c => s"${c._1} ${c._2}").mkString(", ")
    val ddl = s"""
      CREATE TABLE $database.$table (
        stringfield string
      ) PARTITIONED BY ($partitionColumnsDdl)
      STORED AS PARQUET
      """

    Hive.query(ddl).run(hiveConf)
    executesOk(write)

    val path = hiveWarehouse </> s"$database.db" </> table

    facts(
      path ==> exists,
      path </> s"p_domain=rdbms_changes" </> "*" </> "*" </> "*.parquet" ==> records(
        ParquetThermometerRecordReader[SimpleHive], data
      )
    )
  }
}

object TestHelper {
  def change(s: SimpleHive): SimpleHive2 =
    SimpleHive2(s.stringfield + "1")
}

object ParquetThermometerRecordReader {
  def apply[A <: ThriftStruct : Manifest] =
    ThermometerRecordReader((conf, path) => IO {
      ParquetScroogeTools.listFromPath[A](conf, path) })
}

