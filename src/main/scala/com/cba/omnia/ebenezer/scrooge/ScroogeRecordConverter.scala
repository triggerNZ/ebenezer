package com.cba.omnia.ebenezer
package scrooge

import org.apache.thrift.TException
import org.apache.thrift.protocol.TProtocol

import com.cba.omnia.ebenezer.reflect._
import com.twitter.scrooge.ThriftStruct
import com.twitter.scrooge.ThriftStructCodec

import parquet.schema.MessageType
import parquet.thrift.ThriftReader
import parquet.thrift.ThriftRecordConverter
import parquet.thrift.struct.ThriftType.StructType

class ScroogeRecordConverter[A <: ThriftStruct](cls: Class[A], parquetSchema: MessageType, thriftType: StructType) extends ThriftRecordConverter[A](new ThriftReader[A] {
  val codec = Reflect.companionOf(cls).asInstanceOf[ThriftStructCodec[A]]
  def readOneRecord(protocol: TProtocol): A =
    codec.decode(protocol)
}, cls.getSimpleName, parquetSchema, thriftType) {
}
