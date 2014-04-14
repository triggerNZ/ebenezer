package au.com.cba.omnia.ebenezer
package scrooge

import cascading.flow.FlowDef
import cascading.tuple.Fields
import com.twitter.scalding._
import com.twitter.scalding.TDsl._
import com.twitter.scrooge._

import au.com.cba.omnia.ebenezer.test._
import au.com.cba.omnia.thermometer.core._, Thermometer._
import au.com.cba.omnia.thermometer.tools._
import au.com.cba.omnia.thermometer.fact._, PathFactoids._

import org.apache.hadoop.mapred.JobConf

import scalaz.effect.IO

object PartitionParquetScroogeSourceSpec extends ThermometerSpec { def is = s2"""

PartitionParquetScroogeSource usage
==================================

  Write to partitioned parquet w/ single field        $single
  Write to partitioned parquet                        $write

"""
  val data = List(
    Customer("CUSTOMER-1", "Fred", "Bedrock", 40),
    Customer("CUSTOMER-2", "Wilma", "Bedrock", 40),
    Customer("CUSTOMER-3", "Barney", "Bedrock", 39),
    Customer("CUSTOMER-4", "BamBam", "Bedrock", 2)
  )

  def write =
    ThermometerSource(data)
      .map(customer => (customer.address, customer.age)  -> customer)
      .write(PartitionParquetScroogeSource[(String, Int), Customer]("%s/%s", "partitioned"))
      .withFacts(
        "partitioned" </> "Bedrock" </> "40" </> "*.parquet"  ==> recordCount(ParquetThermometerRecordReader[Customer], 2)
      , "partitioned" </> "Bedrock" </> "39" </> "*.parquet"  ==> recordCount(ParquetThermometerRecordReader[Customer], 1)
      , "partitioned" </> "Bedrock" </> "2"  </> "*.parquet"  ==> recordCount(ParquetThermometerRecordReader[Customer], 1)
      )

  def single =
    ThermometerSource(data)
      .map(customer => customer.age  -> customer)
      .write(PartitionParquetScroogeSource[Int, Customer]("%s", "partitioned"))
      .withFacts(
        "partitioned" </> "40" </> "*.parquet"  ==> recordCount(ParquetThermometerRecordReader[Customer], 2)
      , "partitioned" </> "39" </> "*.parquet"  ==> recordCount(ParquetThermometerRecordReader[Customer], 1)
      , "partitioned" </>  "2"  </> "*.parquet"  ==> recordCount(ParquetThermometerRecordReader[Customer], 1)
      )

}
