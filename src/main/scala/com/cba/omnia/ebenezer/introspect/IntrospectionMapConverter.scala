package com.cba.omnia.ebenezer
package introspect

import parquet.io.api.Binary
import parquet.io.api.GroupConverter
import parquet.schema.GroupType
import parquet.schema.OriginalType
import scala.collection.JavaConverters._
import scala.collection.immutable.Map
import scalaz._, Scalaz._

class IntrospectionMapConverter(val schema: GroupType, done: Map[Value, Value] => Unit) extends GroupConverter {
  val values: scala.collection.mutable.ListBuffer[(Value, Value)] = scala.collection.mutable.ListBuffer()
  val fields = schema.getFields.asScala.toList

  override def start = { }
  override def end = { done(Map(values: _*)) }

  override def getConverter(n: Int) =
    converters(n)

  lazy val converters = fields.map({
    case field if field.getOriginalType == OriginalType.MAP_KEY_VALUE =>
      new IntrospectionRecordConverter(field.asGroupType, record => {
        val key = record.data.find(_.name == "key").map(_.value)
        val value = record.data.find(_.name == "value").map(_.value)
        val pair = key.tuple(value)
        values += pair.getOrElse(sys.error("Missing key <$key> or value <$value>, expected both for complete map entry."))
      })
    case field =>
      sys.error("Unexpected field, currently reading a map, and expected an underlying OriginalType.MAP_KEY_VALUE and nothing else")
  })

}
