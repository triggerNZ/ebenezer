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

package au.com.cba.omnia.ebenezer.scrooge.hive

import scala.collection.JavaConverters._

import org.specs2.Specification
import org.specs2.matcher.ThrownExpectations

class ParquetTableDescriptorSpec extends Specification with ThrownExpectations { def is = s2"""

ParquetTableDescriptor
======================

  creates a Hive table with parquet serialisation $serial
  creates a Hive table with parquet input format  $inputFormat
  creates a Hive table with parquet output format $outputFormat

"""

  val descriptor = new ParquetTableDescriptor(
    "test", "test",
    Array("x", "y", "l", "m"),
    Array("string", "string", "List < String >", "MAP < int, string >"),
    Array()
  )
  val table      = descriptor.toHiveTable
  val sd         = table.getSd

  def serial =
    sd.getSerdeInfo.getSerializationLib === ParquetTableDescriptor.PARQUET_SERIALIZATION_LIB

  def inputFormat =
    sd.getInputFormat === ParquetTableDescriptor.PARQUET_INPUT_FORMAT
    
  def outputFormat =
    sd.getOutputFormat === ParquetTableDescriptor.PARQUET_OUTPUT_FORMAT
}
