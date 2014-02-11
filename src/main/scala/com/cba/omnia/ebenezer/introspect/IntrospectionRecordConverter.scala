package com.cba.omnia.ebenezer
package introspect

import parquet.io.api.Binary
import parquet.io.api.GroupConverter
import parquet.schema.GroupType

import scala.collection.JavaConverters._

class IntrospectionRecordConverter(val schema: GroupType, done: Record => Unit = identity) extends GroupConverter {
  val builder: RecordBuilder = RecordBuilder()
  val fields = schema.getFields.asScala.toList
  var current: Option[Record] = None

  def fail =
    sys.error("something really bad happended and it is hadoops fault so don't feel bad")

  def getCurrentRecord: Record =
    current.getOrElse(fail)

  override def start = { }
  override def end = { current = Option(builder.toRecord); done(getCurrentRecord) }

  override def getConverter(n: Int) =
    fields(n) match {
      case field if field.isPrimitive =>
        new IntrospectionPrimitiveConverter(field.getName, builder)
      case field =>
        new IntrospectionRecordConverter(field.asGroupType, record =>
          builder.add(field.getName, RecordValue(record)))
    }
}
