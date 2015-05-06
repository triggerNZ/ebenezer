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

import scala.collection.mutable.ListBuffer
import scala.collection.JavaConverters._

import parquet.io.api.{Binary, GroupConverter}
import parquet.schema.{GroupType, OriginalType}

import scalaz._, Scalaz._

class IntrospectionListConverter(val schema: GroupType, done: List[Value] => Unit) extends GroupConverter {
  val values: ListBuffer[Value] = ListBuffer()
  val fields = schema.getFields.asScala.toList

  override def start = { }
  override def end = { done(values.toList) <| (_ => values.clear) }

  override def getConverter(n: Int) =
    converters(n)

  lazy val converters = fields.map({
    case field if field.isPrimitive && field.getOriginalType == OriginalType.ENUM =>
      new IntrospectionEnumConverter(values.append(_))
    case field if field.isPrimitive =>
      new IntrospectionPrimitiveConverter(values.append(_))
    case field if field.getOriginalType == OriginalType.LIST =>
      new IntrospectionListConverter(field.asGroupType, nested => values.append(ListValue(nested)))
    case field if field.getOriginalType == OriginalType.MAP =>
      new IntrospectionMapConverter(field.asGroupType, value => values.append(MapValue(value)))
    case field =>
      new IntrospectionRecordConverter(field.asGroupType, record => values.append(RecordValue(record)))
  })

}
