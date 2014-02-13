package com.cba.omnia.ebenezer
package scrooge

import com.cba.omnia.ebenezer.reflect._

import com.twitter.scrooge.ThriftStruct
import com.twitter.scrooge.ThriftStructCodec
import com.twitter.scrooge.ThriftStructField
import com.twitter.scrooge.ThriftStructMetaData

import java.lang.reflect.ParameterizedType
import java.lang.reflect.Type

import parquet.thrift.struct.ThriftField
import parquet.thrift.struct.ThriftType
import parquet.thrift.struct.ThriftTypeID
import parquet.thrift.struct.ThriftTypeID._

import scala.collection.JavaConverters._

class ScroogeStructConverter {
  def convert[A <: ThriftStruct](cls: Class[A]): ThriftType.StructType = {
    val companion = Reflect.companionOf(cls).asInstanceOf[ThriftStructCodec[_ <: ThriftStruct]]
    fromCodec(companion)
  }

  def fromCodec(codec: ThriftStructCodec[_ <: ThriftStruct]): ThriftType.StructType = {
    val fields = stripEnumDuplicates(codec.metaData.fields.toList).map(toThriftField)
    new ThriftType.StructType(fields.asJava)
  }

  def toThriftField(field: ThriftStructField[_]) = {
    val requirement = if (isOptional(field)) ThriftField.Requirement.OPTIONAL else ThriftField.Requirement.REQUIRED
    val typeId = ThriftTypeID.fromByte(field.tfield.`type`)
    val thriftType = typeId match {
      case BOOL   => new ThriftType.BoolType
      case BYTE   => new ThriftType.ByteType
      case DOUBLE => new ThriftType.DoubleType
      case I16    => new ThriftType.I16Type
      case I32    => new ThriftType.I32Type
      case I64    => new ThriftType.I64Type
      case STRING => new ThriftType.StringType
      case STRUCT => struct(field)
      case MAP    => map(field, requirement)
      case SET    => set(field, requirement)
      case LIST   => list(field, requirement)
      case ENUM   => enum(field)
      case _      => sys.error(s"Can't convert thrift type id <$typeId>")
    }
    new ThriftField(field.tfield.name, field.tfield.id, requirement, thriftType)
  }

  def set(field: ThriftStructField[_], requirement: ThriftField.Requirement): ThriftType = {
    val args: List[Class[_]] = argsOf(field)
    val elementType = toThriftType(args.head)
    val elementField = fieldOf(field.name, requirement, elementType)
    new ThriftType.SetType(elementField)
  }

  def list(field: ThriftStructField[_], requirement: ThriftField.Requirement): ThriftType = {
    val args = argsOf(field)
    val elementType = toThriftType(args.head)
    val elementField = fieldOf(field.name, requirement, elementType)
    new ThriftType.ListType(elementField)
  }

  def map(field: ThriftStructField[_], requirement: ThriftField.Requirement): ThriftType = {
    val args = argsOf(field)
    val keyType = toThriftType(args.head)
    val keyField = fieldOf(field.name + "_map_key", requirement, keyType)
    val valueType = toThriftType(args.tail.head)
    val valueField = fieldOf(field.name + "_map_value", requirement, valueType)
    new ThriftType.MapType(keyField, valueField)
  }

  def enum(f: ThriftStructField[_]): ThriftType = {
    val companion = Reflect.companionOfName(f.method.getReturnType.getName)
    val enums = Reflect.invoke[Seq[AnyRef]](companion, "list").toList
    val values = enums.map(raw => {
      val id = Reflect.invoke[Int](raw, "value")
      val name = Reflect.invoke[String](raw, "name")
      new ThriftType.EnumValue(id, name.toUpperCase)
    })
    new ThriftType.EnumType(values.asJava)
  }

  def struct(f: ThriftStructField[_]): ThriftType = {
    val classType =
      if (isOptional(f))
        Reflect.parameterizedTypeOf1(f.method.getGenericReturnType)
      else
        f.method.getReturnType
    convert(classType.asInstanceOf[Class[_ <: ThriftStruct]])
  }

  def fieldOf(name: String, requirement: ThriftField.Requirement, thriftType: ThriftType): ThriftField =
    new ThriftField(name, 1, requirement, thriftType)

  def toThriftType(cls: Class[_]): ThriftType =
    if (classOf[Boolean] == cls)
      new ThriftType.BoolType
    else if (classOf[Byte] == cls)
      new ThriftType.ByteType
    else if (classOf[Double] == cls)
      new ThriftType.DoubleType
    else if (classOf[Short] == cls)
      new ThriftType.I16Type()
    else if (classOf[Int] == cls)
      new ThriftType.I32Type()
    else if (classOf[Long] == cls)
      new ThriftType.I64Type()
    else if (classOf[String] == cls)
      new ThriftType.StringType()
    else
      convert(cls.asInstanceOf[Class[_ <: ThriftStruct]])

  def isOptional(f: ThriftStructField[_]): Boolean =
    f.method.getReturnType() == classOf[Option[_]]

  def argsOf(field: ThriftStructField[_]): List[Class[_]] =
    field.manifest.get.typeArguments.map(_.runtimeClass)

  /*
   * Ere' be dragons. Scrooge generates two fields for an ENUM,
   *  1. The actual enum field.
   *  2. An I32 of the same name.
   *
   * It is not entirely clear how this should be handled generally,
   * but the code looks like it generated specifically to support
   * different thift generators, some of which send ENUM and some of
   * which send I32.
   *
   * For deriving the parquet schema type we must only use one of the
   * fields, and it makes the most sense for that to be the ENUM
   * version of it.
   */
  def stripEnumDuplicates(fields: List[ThriftStructField[_]]): List[ThriftStructField[_]] =
    fields.groupBy(_.tfield.id).values.toList.flatMap({
      case xs @ x :: y :: Nil =>
        val xType = ThriftTypeID.fromByte(x.tfield.`type`)
        val yType = ThriftTypeID.fromByte(y.tfield.`type`)
        if (xType == ENUM && yType == I32)
          List(x)
        else if (xType == I32 && yType == ENUM)
          List(y)
        else
          xs
      case xs =>
        xs
    }).sortBy(_.tfield.id)
}
