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

import org.apache.thrift.protocol.TProtocol

import com.twitter.scrooge.{ThriftStruct, ThriftStructCodec}

import parquet.schema.MessageType
import parquet.thrift.{ThriftReader, ThriftRecordConverter}
import parquet.thrift.struct.ThriftType.StructType

import au.com.cba.omnia.ebenezer.reflect._

class ScroogeRecordConverter[A <: ThriftStruct](cls: Class[A], parquetSchema: MessageType, thriftType: StructType) extends ThriftRecordConverter[A](new ThriftReader[A] {
  val codec = Reflect.companionOf(cls).asInstanceOf[ThriftStructCodec[A]]
  def readOneRecord(protocol: TProtocol): A =
    codec.decode(protocol)
}, cls.getSimpleName, parquetSchema, thriftType) {
}
