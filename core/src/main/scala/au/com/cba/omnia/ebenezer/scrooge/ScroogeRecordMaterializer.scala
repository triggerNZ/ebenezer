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
