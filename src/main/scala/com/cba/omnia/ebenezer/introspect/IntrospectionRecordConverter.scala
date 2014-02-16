package com.cba.omnia.ebenezer
package introspect

import parquet.io.api.Binary
import parquet.io.api.GroupConverter
import parquet.schema.GroupType
import parquet.schema.OriginalType
import scala.collection.JavaConverters._
import scalaz._, Scalaz._

class IntrospectionRecordConverter(val schema: GroupType, done: Record => Unit) extends GroupConverter {
  val builder: RecordBuilder = RecordBuilder()
  val fields = schema.getFields.asScala.toList

  override def start = { }
  override def end = { done(builder.toRecord) <| (_ => builder.clear) }

  override def getConverter(n: Int) =
    converters(n)

  lazy val converters = fields.map({
    case field if field.isPrimitive && field.getOriginalType == OriginalType.ENUM =>
      new IntrospectionEnumConverter(value => builder.add(field.getName, value))
    case field if field.isPrimitive =>
      new IntrospectionPrimitiveConverter(value => builder.add(field.getName, value))
    case field if field.getOriginalType == OriginalType.LIST =>
      new IntrospectionListConverter(field.asGroupType, value => builder.add(field.getName, ListValue(value)))
    case field if field.getOriginalType == OriginalType.MAP =>
      new IntrospectionMapConverter(field.asGroupType, value => builder.add(field.getName, MapValue(value)))
    case field =>
      new IntrospectionRecordConverter(field.asGroupType, record =>
        builder.add(field.getName, RecordValue(record)))
  })
}
