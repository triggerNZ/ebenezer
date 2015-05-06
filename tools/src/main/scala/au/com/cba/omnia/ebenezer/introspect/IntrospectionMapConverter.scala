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

import scala.collection.immutable.Map
import scala.collection.mutable.ListBuffer
import scala.collection.JavaConverters._

import parquet.io.api.{Binary, GroupConverter}
import parquet.schema.{GroupType, OriginalType}

import scalaz._, Scalaz._

class IntrospectionMapConverter(val schema: GroupType, done: Map[Value, Value] => Unit) extends GroupConverter {
  val values: ListBuffer[(Value, Value)] = ListBuffer()
  val fields = schema.getFields.asScala.toList

  override def start = { }
  override def end = { done(Map(values: _*)) <| (_ => values.clear) }

  override def getConverter(n: Int) =
    converters(n)

  lazy val converters = fields.map({
    case field if field.getOriginalType == OriginalType.MAP_KEY_VALUE =>
      new IntrospectionRecordConverter(field.asGroupType, record => {
        val key = record.data.find(_.name == "key").map(_.value)
        val value = record.data.find(_.name == "value").map(_.value)
        val pair = key.tuple(value)
        values.append(
          pair.getOrElse(sys.error(s"Missing key <$key> or value <$value>, expected both for complete map entry."))
        )
      })
    case field =>
      sys.error("Unexpected field, currently reading a map, and expected an underlying OriginalType.MAP_KEY_VALUE and nothing else")
  })

}
