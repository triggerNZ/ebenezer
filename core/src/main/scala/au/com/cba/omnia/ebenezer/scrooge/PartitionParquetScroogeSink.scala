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

import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.OutputCollector
import org.apache.hadoop.mapred.RecordReader

import cascading.tap.Tap

import com.twitter.scalding._
import com.twitter.scalding.typed.PartitionUtil

import com.twitter.scrooge.ThriftStruct

/**
  * A scalding sink to write out Scrooge Thrift structs as partitioned files using parquet as
  * underlying storage format.
  *
  * It expects tuples where the first part is the partition and the second part the values to write
  * out.
  *
  * Unfortunately read does not work since the ParquetInputSplit is an instance of
  * mapreduce.FileSplit and cascading will ignore any partitioned input splits that aren't part of
  * mapred.FileSplit.
  * Instead use [[PartitionParquetScroogeSource]] for read.
  *
  * @param template for the partition directory where `%s` is the placeholder for the partition
  *   values e.g. `"col1=%s/col2=%s"`
  * @param path the top level directory to write the partitions to
  */
case class PartitionParquetScroogeSink[A, T <: ThriftStruct](template: String, path: String)(
  implicit m: Manifest[T], valueSet: TupleSetter[T], partitionSet: TupleSetter[A]
) extends Source
  with TypedSink[(A, T)]
  with java.io.Serializable {

  val partition = {
    val templateFields = PartitionUtil.toFields(valueSet.arity, valueSet.arity + partitionSet.arity)
    new TemplatePartition(templateFields, template)
  }

  val hdfsScheme = ParquetScroogeSchemeSupport.parquetHdfsScheme[T]
  hdfsScheme.setSinkFields(Dsl.strFields(List("0")))

  override def createTap(readOrWrite: AccessMode)(implicit mode: Mode): Tap[_, _, _] = mode match {
    case hdfsMode @ Hdfs(_, jobConf) => readOrWrite match {
      case Write => { 
        val tap = new PartitionParquetScroogeWriteTap(path, partition, hdfsScheme)
        tap.asInstanceOf[Tap[JobConf, RecordReader[_, _], OutputCollector[_, _]]]
      }
      case Read  =>
        sys.error(s"HDFS read mode is currently not supported for ${toString}. Use PartitionParquetScroogeSource instead.")
    }
    case Local(_) => sys.error(s"Local mode is currently not supported for ${toString}")
    case x        => sys.error(s"$x mode is currently not supported for ${toString}")
  }

  override def sinkFields =
    PartitionUtil.toFields(0, valueSet.arity + partitionSet.arity)

  /** Sets the setter to flatten the values and partition parts into a cascading tuple. */
  override def setter[U <: (A, T)] = PartitionUtil.setter[A, T, U](valueSet, partitionSet)

  override def toString: String =
    s"PartitionParquetScroogeSink[${m.runtimeClass}]($template, $path)"
}
