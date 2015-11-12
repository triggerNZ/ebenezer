// //   Copyright 2014 Commonwealth Bank of Australia
// //
// //   Licensed under the Apache License, Version 2.0 (the "License");
// //   you may not use this file except in compliance with the License.
// //   You may obtain a copy of the License at
// //
// //     http://www.apache.org/licenses/LICENSE-2.0
// //
// //   Unless required by applicable law or agreed to in writing, software
// //   distributed under the License is distributed on an "AS IS" BASIS,
// //   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// //   See the License for the specific language governing permissions and
// //   limitations under the License.

package au.com.cba.omnia.ebenezer.scrooge.hive

import scala.collection.JavaConverters._

import org.apache.hadoop.hive.metastore.api.FieldSchema

import org.specs2.Specification
import org.specs2.matcher.ThrownExpectations

import org.apache.hadoop.hive.metastore.api.StorageDescriptor

class HiveMetadataTableSpec extends Specification with ThrownExpectations { def is = s2"""

HiveMetadataTableSpec
=====================

  create a valid  table for primitive types      $primitives
  create a valid table for list                  $list
  create a valid table for map                   $map
  create a valid table for nested maps and lists $nested

"""

  val com = "Created by Ebenezer"

  def primitives =  {
    val expected = List(
      new FieldSchema("boolean", "boolean", com), new FieldSchema("bytey", "tinyint", com),
      new FieldSchema("short", "smallint", com), new FieldSchema("integer", "int", com),
      new FieldSchema("long", "bigint", com), new FieldSchema("doubley", "double", com),
      new FieldSchema("stringy", "string", com)
    )

    val td = HiveMetadataTable[Primitives]("db", "test", List.empty, ParquetFormat)
    val actual = td.getSd.getCols.asScala.toList

    actual must_== expected
  }

  def list =  {
    val expected = List(
      new FieldSchema("short", "smallint", com), new FieldSchema("listy", "array<int>", com)
    )

    val td = HiveMetadataTable[Listish]("db", "test", List.empty, ParquetFormat)
    val sd = td.getSd
    val actual = sd.getCols.asScala.toList

    verifyInputOutputFormatForParquet(sd)
    actual must_== expected
  }

  def map =  {
    val expected = List(
      new FieldSchema("short", "smallint", com), new FieldSchema("mapy", "map<int,string>", com)
    )

    val td = HiveMetadataTable[Mapish]("db", "test", List.empty, ParquetFormat)
    val actual = td.getSd.getCols.asScala.toList

    actual must_== expected
  }

  def nested =  {
    val expected = List(
      new FieldSchema("nested", "map<int,map<string,array<int>>>", com)
    )

    val td = HiveMetadataTable[Nested]("db", "test", List.empty, ParquetFormat)
    val sd = td.getSd

    val actual = sd.getCols.asScala.toList

    verifyInputOutputFormatForParquet(sd)
    actual must_== expected
  }

  def verifyInputOutputFormatForParquet(sd: StorageDescriptor) = {
    sd.getInputFormat()  must_== ParquetFormat.inputFormat
    sd.getOutputFormat() must_== ParquetFormat.outputFormat
  }
}
