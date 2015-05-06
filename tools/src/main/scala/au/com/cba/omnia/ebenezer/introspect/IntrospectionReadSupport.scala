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

package au.com.cba.omnia.ebenezer.introspect

import java.util.{Map => JMap}

import org.apache.hadoop.conf.Configuration

import parquet.hadoop.api.{InitContext, ReadSupport}, ReadSupport._
import parquet.schema.MessageType

class IntrospectionReadSupport extends ReadSupport[Record] {
  override def prepareForRead(conf: Configuration, metaData: JMap[String, String], schema: MessageType, context: ReadContext) =
    new IntrospectionMaterializer(schema)

  override def init(context: InitContext): ReadContext =
    new ReadContext(context.getFileSchema)
}
