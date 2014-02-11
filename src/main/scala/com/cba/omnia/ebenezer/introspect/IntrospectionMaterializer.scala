package com.cba.omnia.ebenezer
package introspect

import parquet.io.api.RecordMaterializer
import parquet.schema.MessageType

class IntrospectionMaterializer(schema: MessageType) extends RecordMaterializer[Record] {
  val converter = new IntrospectionRecordConverter(schema)
  def getCurrentRecord = converter.getCurrentRecord
  def getRootConverter = converter
}
