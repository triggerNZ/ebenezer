//   Copyright 2014 Commonwealth Bank of Australia
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.

package au.com.cba.omnia.ebenezer
package scrooge

import au.com.cba.omnia.thermometer.core._, Thermometer._
import au.com.cba.omnia.thermometer.fact.PathFactoids._

import au.com.cba.omnia.ebenezer.ParquetLogging
import au.com.cba.omnia.ebenezer.test._

object PartitionParquetScroogeSourceSpec extends ThermometerSpec with ParquetLogging { def is = s2"""

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
