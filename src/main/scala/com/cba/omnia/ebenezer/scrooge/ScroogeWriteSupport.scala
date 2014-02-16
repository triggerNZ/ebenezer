package com.cba.omnia.ebenezer
package scrooge

import com.twitter.scrooge.ThriftStruct

import org.apache.hadoop.conf.Configuration

import parquet.hadoop.api.WriteSupport
import parquet.hadoop.api.WriteSupport.WriteContext
import parquet.io.ColumnIOFactory
import parquet.io.api.RecordConsumer
import parquet.schema.MessageType
import parquet.thrift.ParquetWriteProtocol
import parquet.thrift.ThriftSchemaConverter
import parquet.thrift.struct.ThriftType.StructType

class ScroogeWriteSupport[A <: ThriftStruct] extends WriteSupport[A] {
  /* these rely on the WriteSupport lifecycle, it is horrible and unpleasant,
     but they are left as nullable fields to avoid unpacking on every write
     (with no recourse for failure anyway) */
  var schema: MessageType = null
  var struct: StructType = null
  var parquetWriteProtocol: ParquetWriteProtocol = null

  def init(config: Configuration): WriteContext = {
    val thrift = ScroogeReadWriteSupport.getThriftClass[A](config)
    val converter = new ScroogeStructConverter
    struct = converter.convert(thrift)
    schema =  new ThriftSchemaConverter().convert(struct)
    val extra = new java.util.HashMap[String, String]
    extra.put("thrift.class", thrift.getName)
    extra.put("thrift.descriptor", struct.toJSON)
    new WriteContext(schema, extra)
  }

  def prepareForWrite(consumer: RecordConsumer): Unit = {
    val io = new ColumnIOFactory().getColumnIO(schema)
    parquetWriteProtocol = new ParquetWriteProtocol(consumer, io, struct)
  }

  def write(record: A): Unit =
    record.write(parquetWriteProtocol)
}
