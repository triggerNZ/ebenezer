package au.com.cba.omnia.ebenezer
package scrooge

import au.com.cba.omnia.thermometer.core._, Thermometer._
import au.com.cba.omnia.thermometer.fact.PathFactoids._

import au.com.cba.omnia.ebenezer.test._

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
      .write(PartitionParquetScroogeSource[(String, Int), Customer]("address=%s/age=%s", "partitioned"))
      .withFacts(
        "partitioned" </> "address=Bedrock" </> "age=40" </> "*.parquet"  ==> recordCount(ParquetThermometerRecordReader[Customer], 2)
      , "partitioned" </> "address=Bedrock" </> "age=39" </> "*.parquet"  ==> recordCount(ParquetThermometerRecordReader[Customer], 1)
      , "partitioned" </> "address=Bedrock" </> "age=2"  </> "*.parquet"  ==> recordCount(ParquetThermometerRecordReader[Customer], 1)
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
