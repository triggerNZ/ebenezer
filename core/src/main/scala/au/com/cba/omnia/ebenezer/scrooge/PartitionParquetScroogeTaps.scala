//   Copyright 2015 Commonwealth Bank of Australia
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

import com.twitter.scalding.CastHfsTap

import scala.collection.JavaConversions._

import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.OutputCollector
import org.apache.hadoop.mapred.RecordReader

import cascading.scheme.Scheme
import cascading.tap.{Tap, SinkMode}
import cascading.tap.hadoop.{PartitionTap, Hfs}
import cascading.tap.partition.Partition

/**
  * Custom tap for reading partitioned parquet files.
  * 
  * See [[PartitionParquetScroogeSource]] for use. This will add globs for all the
  * underlying partitions to the path of `Hfs`.
  */
class PartitionParquetScroogeReadTap(
  path: String, partition: Partition, hdfsScheme: Scheme[JobConf, RecordReader[_, _], OutputCollector[_, _], _, _]
) extends Hfs(hdfsScheme, PartitionParquetScroogeTap.createGlobbedPath(path, partition), SinkMode.KEEP)

/**
  * Custom tap for writing partitioned parquet files., see [[PartitionParquetScroogeSink]]
  *
  * Since [[PartitionHiveParquetReadTap]] sets a custom path that includes the partitions as globbed
  * directories this ensures that the identifiers between reading and writing are the same and,
  * therefore, Cascading can identify the dependency between them correctly.
  */
class PartitionParquetScroogeWriteTap(
  path: String, partition: Partition, hdfsScheme: Scheme[JobConf, RecordReader[_, _], OutputCollector[_, _], _, _]
) extends PartitionTap(
    new Hfs(hdfsScheme, path, SinkMode.REPLACE), partition, SinkMode.UPDATE
) {
  
  override def getIdentifier() =
    PartitionParquetScroogeTap.createGlobbedPath(super.getIdentifier(), partition)

  override def getFullIdentifier(conf: JobConf) = {
    //Force the `getFullIdentifier` call on the parent Hfs which points to the path.
    val parentHfs = parent.asInstanceOf[Tap[JobConf, RecordReader[_, _], OutputCollector[_, _]]] //Type Resolution fix
    PartitionParquetScroogeTap.createGlobbedPath(parentHfs.getFullIdentifier(conf), partition)
  }
}

/** Provides utility functions for the PartitionParquetScroogeTaps. */
object PartitionParquetScroogeTap {
  /** Creates a path glob for all the files in a partitioned hive table. */
  def createGlobbedPath(path: String, partition: Partition) =
    s"$path/${partitionColumnsGlob(partition)}"

  /** Creates globs (`*`) for all the partition directories. */
  def partitionColumnsGlob(partition: Partition) =
    partition.getPartitionFields().iterator.map(_ => "*").mkString("/")
}
