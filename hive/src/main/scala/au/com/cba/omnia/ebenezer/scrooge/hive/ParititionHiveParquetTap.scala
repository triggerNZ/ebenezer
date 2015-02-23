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

import org.apache.hadoop.mapred.JobConf

import cascading.scheme.Scheme
import cascading.tap.{Tap, SinkMode}
import cascading.tap.hive.{HiveTap, HivePartitionTap, HiveTableDescriptor}

/**
  * Custom tap for reading partitioned hive parquet tables.
  * 
  * See [[PartitionHiveParquetScroogeSink]] for why this is needed. This will add globs for all the
  * underlying partitions to the path of the hive table.
  */
class PartitionHiveParquetReadTap(
  tableDescriptor: HiveTableDescriptor, hdfsScheme: Scheme[_, _, _, _, _]
) extends HiveTap(tableDescriptor, hdfsScheme, SinkMode.KEEP, true){

  override def setStringPath(path: String): Unit = {
    super.setStringPath(PartitionHiveParquetTap.createGlobbedPath(path, tableDescriptor))
  }
}

/**
  * Custom tap for writing partitioned hive parquet tables., see [[PartitionHiveParquetScroogeSink]]
  *
  * Since [[PartitionHiveParquetReadTap]] sets a custom path that includes the partitions as globbed
  * directories this ensures that the identifiers between reading and writing are the same and,
  * therefore, Cascading can identify the dependency between them correctly.
  */
class PartitionHiveParquetWriteTap(
  tableDescriptor: HiveTableDescriptor, hdfsScheme: Scheme[_, _, _, _, _]
) extends HivePartitionTap(
  new HiveTap(tableDescriptor, hdfsScheme, SinkMode.REPLACE, true), SinkMode.UPDATE
) {
  override def getIdentifier() =
    PartitionHiveParquetTap.createGlobbedPath(super.getIdentifier(), tableDescriptor)

  override def getFullIdentifier(conf: JobConf ) =
    PartitionHiveParquetTap.createGlobbedPath(super.getFullIdentifier(conf), tableDescriptor)
}

/** Provides utility functions for the PartitionHiveParquetTaps. */
object PartitionHiveParquetTap {
  /** Creates a path glob for all the files in a partitioned hive table. */
  def createGlobbedPath(path: String, tableDescriptor: HiveTableDescriptor) =
    s"$path/${partitionColumnsGlob(tableDescriptor)}"

  /** Creates globs (`*`) for all the partition directories. */
  def partitionColumnsGlob(tableDescriptor: HiveTableDescriptor) =
    tableDescriptor.getPartitionKeys.map(_ => "*").mkString("/")
}
