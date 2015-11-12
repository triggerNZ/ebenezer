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

import org.apache.hadoop.mapred.JobConf

import cascading.tap.Tap

import com.twitter.scalding._, TDsl._
import com.twitter.scalding.typed.IterablePipe

import au.com.cba.omnia.thermometer.core._, Thermometer._
import au.com.cba.omnia.thermometer.fact.PathFactoids._

import au.com.cba.omnia.ebenezer.ParquetLogging
import au.com.cba.omnia.ebenezer.test._

object PartitionParquetScroogeSpec extends ThermometerSpec with ParquetLogging { def is = s2"""

PartitionParquetScrooge Source and Sink test
============================================

  Allows writing to partitioned parquet              $writeTest
  Allows writing over existing partitioned parquet   $writeOnExistingTest
  Allows reading from partitioned parquet            $readTest
  Can match identifier across source and sink        $identifierTest
"""
  val basePath = "customer"
  val partitionTemplate = "address=%s/age=%s"

  val data = List(
    Customer("CUSTOMER-1", "Fred", "Bedrock", 40),
    Customer("CUSTOMER-2", "Wilma", "Bedrock", 40),
    Customer("CUSTOMER-3", "Barney", "Bedrock", 39),
    Customer("CUSTOMER-4", "BamBam", "Fragglerock", 2)
  )

  def stripPrefix(prefix: String, str: String) =
    if (str.startsWith(prefix)) str.substring(prefix.length) else str

  def customerNew(c: Customer) =
    CustomerNew(stripPrefix("CUSTOMER-", c.id).toInt, c.name, c.address)

  def writeWith(customers: Customer*) = {
    IterablePipe(customers)
      .map(customer => (customer.address, customer.age)  -> customer)
      .writeExecution(PartitionParquetScroogeSink[(String, Int), Customer](partitionTemplate, basePath))
  }

  def write = writeWith(data: _*)

  def read = {
    PartitionParquetScroogeSource[(String, Int), Customer](partitionTemplate, basePath)
      .map(c =>  customerNew(c)).map(cn => cn.address -> cn)
      .writeExecution(PartitionParquetScroogeSink[String, CustomerNew]("%s", "customernew"))
  }

  def writeTest = {
    executesOk(write)

    facts(
      basePath </> "address=Bedrock"     </> "age=40" </> "*.parquet"  ==> recordCount(ParquetThermometerRecordReader[Customer], 2)
    , basePath </> "address=Bedrock"     </> "age=39" </> "*.parquet"  ==> recordCount(ParquetThermometerRecordReader[Customer], 1)
    , basePath </> "address=Fragglerock" </> "age=2"  </> "*.parquet"  ==> recordCount(ParquetThermometerRecordReader[Customer], 1)
    )
  }

  def writeOnExistingTest = {
    executesOk(write)
    executesOk(writeWith(Customer("CUSTOMER-5", "Tom", "Fragglerock", 10)))

    facts(
      basePath </> "address=Bedrock"     </> "age=40" </> "*.parquet"  ==> recordCount(ParquetThermometerRecordReader[Customer], 2), 
      basePath </> "address=Bedrock"     </> "age=39" </> "*.parquet"  ==> recordCount(ParquetThermometerRecordReader[Customer], 1),
      basePath </> "address=Fragglerock" </> "age=2"  </> "*.parquet"  ==> recordCount(ParquetThermometerRecordReader[Customer], 1),
      basePath </> "address=Fragglerock" </> "age=10" </> "*.parquet"  ==> recordCount(ParquetThermometerRecordReader[Customer], 1)
    )
  }
  
  def readTest = {
    executesOk(write.flatMap(_ => read))

    facts(
      "customernew" </> "Bedrock"     </> "*.parquet"  ==> recordCount(ParquetThermometerRecordReader[CustomerNew], 3),
      "customernew" </> "Fragglerock" </> "*.parquet"  ==> recordCount(ParquetThermometerRecordReader[CustomerNew], 1)
    )
  }

  def identifierTest = {
    val source = PartitionParquetScroogeSource[(String, Int), Customer](partitionTemplate, basePath)
    val sink   = PartitionParquetScroogeSink[(String, Int), Customer](partitionTemplate, basePath)


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

}
