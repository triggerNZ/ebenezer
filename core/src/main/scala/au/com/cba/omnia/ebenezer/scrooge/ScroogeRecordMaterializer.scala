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

import parquet.io.api.RecordMaterializer
import parquet.schema.MessageType
import parquet.thrift.struct.ThriftType.StructType

class ScroogeRecordMaterializer[A <: ThriftStruct](cls: Class[A], schema: MessageType, thrift: StructType) extends RecordMaterializer[A] {
   val root = new ScroogeRecordConverter(cls, schema, thrift)
   def getCurrentRecord = root.getCurrentRecord
   def getRootConverter = root.getRootConverter
}
