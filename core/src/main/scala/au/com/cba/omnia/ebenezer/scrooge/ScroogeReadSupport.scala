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

import com.twitter.scrooge.ThriftStruct

import java.util.{Map => JMap}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred.JobConf

import parquet.io.api.RecordMaterializer
import parquet.schema.MessageType
import parquet.hadoop.ParquetInputFormat
import parquet.hadoop.api.ReadSupport
import parquet.hadoop.api.ReadSupport.ReadContext
import parquet.hadoop.api.InitContext

class ScroogeReadSupport[A <: ThriftStruct] extends ReadSupport[A] {
  override def prepareForRead(conf: Configuration, meta: JMap[String, String], schema: MessageType, context: ReadContext):  RecordMaterializer[A] = {
    val cls = ScroogeReadWriteSupport.getThriftClass[A](conf, ScroogeReadSupport.thriftClass)
    val converter = new ScroogeStructConverter()
    val struct = converter.convert(cls)
    new ScroogeRecordMaterializer(cls, schema, struct)
  }

  override def init(context: InitContext): ReadContext =
    new ReadContext(context.getFileSchema)
}

object ScroogeReadSupport {
  val thriftClass = "parquet.scrooge.read.class";

  def setAsParquetSupportClass[A <: ThriftStruct : Manifest](conf: JobConf) {
    ParquetInputFormat.setReadSupportClass(conf, classOf[ScroogeReadSupport[_]])
    ScroogeReadWriteSupport.setThriftClass[A](conf, thriftClass)
  }
}
