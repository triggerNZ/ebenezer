package com.cba.omnia.ebenezer
package scrooge

import com.twitter.scrooge.ThriftStruct

import java.util.{Map => JMap}

import org.apache.hadoop.conf.Configuration

import parquet.io.api.RecordMaterializer
import parquet.schema.MessageType
import parquet.hadoop.api.ReadSupport
import parquet.hadoop.api.ReadSupport.ReadContext
import parquet.hadoop.api.InitContext
import parquet.thrift.ThriftSchemaConverter
import parquet.thrift.struct.ThriftType.StructType

class ScroogeReadSupport[A <: ThriftStruct] extends ReadSupport[A] {
  override def prepareForRead(conf: Configuration, meta: JMap[String, String], schema: MessageType, context: ReadContext):  RecordMaterializer[A] = {
    val cls = ScroogeReadWriteSupport.getThriftClass[A](conf)
    val converter = new ScroogeStructConverter()
    val struct = converter.convert(cls)
    new ScroogeRecordMaterializer(cls, schema, struct)
  }

  override def init(context: InitContext): ReadContext =
    new ReadContext(context.getFileSchema)
}
