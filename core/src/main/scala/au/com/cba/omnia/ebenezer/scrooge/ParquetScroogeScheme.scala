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
import org.apache.hadoop.mapred.OutputCollector
import org.apache.hadoop.mapred.RecordReader

import parquet.cascading.ParquetValueScheme
import parquet.hadoop.mapred.DeprecatedParquetInputFormat
import parquet.hadoop.mapred.DeprecatedParquetOutputFormat

import cascading.flow.FlowProcess
import cascading.tap.Tap
import cascading.scheme.Scheme

import com.twitter.scalding.HadoopSchemeInstance

import com.twitter.scrooge.ThriftStruct

class ParquetScroogeScheme[A <: ThriftStruct : Manifest] extends ParquetValueScheme[A] {
  override def sinkConfInit(flow: FlowProcess[JobConf], tap: Tap[JobConf, RecordReader[_, _], OutputCollector[_, _]], conf: JobConf): Unit = {
    conf.setOutputFormat(classOf[DeprecatedParquetOutputFormat[_]])
    ScroogeWriteSupport.setAsParquetSupportClass[A](conf)
  }

  override def sourceConfInit(flow: FlowProcess[JobConf], tap: Tap[JobConf, RecordReader[_, _], OutputCollector[_, _]], conf: JobConf): Unit = {
    conf.setInputFormat(classOf[DeprecatedParquetInputFormat[_]])
    ScroogeReadSupport.setAsParquetSupportClass[A](conf)
  }
}

object ParquetScroogeSchemeSupport {
  def parquetHdfsScheme[T <: ThriftStruct](implicit m: Manifest[T]): cascading.scheme.Scheme[JobConf, RecordReader[_, _], OutputCollector[_, _], _, _] =
    HadoopSchemeInstance(new ParquetScroogeScheme[T].asInstanceOf[Scheme[_, _, _, _, _]])
}
