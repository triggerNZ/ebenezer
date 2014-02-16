package com.cba.omnia.ebenezer
package scrooge

import cascading.scheme.Scheme
import cascading.tuple.Fields

import com.twitter.scalding._
import com.twitter.scrooge._

import org.apache.thrift._

import parquet.cascading._

case class ParquetScroogeSource[T <: ThriftStruct](p : String, inFields: Fields = Fields.NONE)(implicit m : Manifest[T], conv: TupleConverter[T], set: TupleSetter[T])
  extends FixedPathSource(p)
  with TypedSink[T]
  with Mappable[T]
  with java.io.Serializable {

  val cls = m.runtimeClass.asSubclass[ThriftStruct](classOf[ThriftStruct])

  override def hdfsScheme = HadoopSchemeInstance(new ParquetScroogeScheme[T].asInstanceOf[Scheme[_, _, _, _, _]])

  override def converter[U >: T] =
    TupleConverter.asSuperConverter[T, U](conv)

  override def setter[U <: T] =
    TupleSetter.asSubSetter[T, U](TupleSetter.of[T])
}
