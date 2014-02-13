package com.cba.omnia.ebenezer
package introspect

import parquet.io.api.RecordMaterializer
import parquet.schema.MessageType

class IntrospectionMaterializer(schema: MessageType) extends RecordMaterializer[Record] {
  var current: Option[Record] = None
  val converter = new IntrospectionRecordConverter(schema, record => current = Option(record))
  def getCurrentRecord = current.getOrElse(fail)
  def getRootConverter = converter
  def fail = sys.error("something really bad happended and it is hadoops fault so don't feel bad")
}
