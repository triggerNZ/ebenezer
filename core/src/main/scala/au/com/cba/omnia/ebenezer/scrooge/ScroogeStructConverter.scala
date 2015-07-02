//   Copyright 2014 Commonwealth Bank of Australia
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.

package au.com.cba.omnia.ebenezer
package scrooge

import scala.collection.{Map => CMap}
import scala.collection.JavaConverters._

import java.lang.reflect.{ParameterizedType, Type}

import com.twitter.scrooge.{ThriftStruct, ThriftStructCodec, ThriftStructField, ThriftStructMetaData}

import parquet.thrift.struct.{ThriftField, ThriftType, ThriftTypeID}
import parquet.thrift.struct.ThriftTypeID._

import au.com.cba.omnia.ebenezer.reflect._

class ScroogeStructConverter(partitionColumns: Set[String] = Set.empty) {
  def convert[A <: ThriftStruct](cls: Class[A]): ThriftType.StructType = {
    val companion = Reflect.companionOf(cls).asInstanceOf[ThriftStructCodec[_ <: ThriftStruct]]
    fromCodec(companion)
  }

  def fromCodec(codec: ThriftStructCodec[_ <: ThriftStruct]): ThriftType.StructType = {
    val fields =
      stripEnumDuplicates(ScroogeMetaHelper.fieldsInStruct(codec).toList)
        .map(toThriftField)
        .filter(f => !partitionColumns.contains(f.getName))
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
    val args: List[Manifest[_]] = argsOf(field)
    val elementType = toThriftType(args.head, field.name, requirement)
    val elementField = fieldOf(field.name, requirement, elementType)
    new ThriftType.SetType(elementField)
  }

  def list(field: ThriftStructField[_], requirement: ThriftField.Requirement): ThriftType = {
    val args = argsOf(field)
    val elementType = toThriftType(args.head, field.name, requirement)
    val elementField = fieldOf(field.name, requirement, elementType)
    new ThriftType.ListType(elementField)
  }

  def map(field: ThriftStructField[_], requirement: ThriftField.Requirement): ThriftType = {
    val args = argsOf(field)
    val keyType = toThriftType(args.head, field.name, requirement)
    val keyField = fieldOf(field.name + "_map_key", requirement, keyType)
    val valueType = toThriftType(args.tail.head, field.name, requirement)
    val valueField = fieldOf(field.name + "_map_value", requirement, valueType)
    new ThriftType.MapType(keyField, valueField)
  }

  def enum(f: ThriftStructField[_]): ThriftType = {
    val companion =
      if (f.manifest.isDefined) {
        Reflect.companionOfName(f.manifest.get.runtimeClass.getName)
      } else {
        Reflect.companionOfName(f.method.getReturnType.getName)
      }
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
      if (f.manifest.isDefined)
        if (isUnionField(f)) {
          // for unions, we need to get the field type (aliased <fieldname>Alias)
          // from the constructor of an intermediate wrapper class (named after the field)
          val wrapperClass   = f.manifest.get.runtimeClass
          val containedValue = wrapperClass.getDeclaredConstructors.head.getParameterTypes.head
          containedValue.asInstanceOf[Class[_ <: ThriftStruct]]
        } else {
          // for other structs, we can use the obtained manifest
          f.manifest.get.runtimeClass // use the manifest inside f (ignores optionality)
        }
      else
      if (isOptional(f))
        Reflect.parameterizedTypeOf1(f.method.getGenericReturnType)
      else
        f.method.getReturnType
    convert(classType.asInstanceOf[Class[_ <: ThriftStruct]])
  }

  /** 
    * Creates a parquet ThriftField of an element in a complex thrift struct such as list.
    * 
    * The id for these fields is 1.
    */
  def fieldOf(name: String, requirement: ThriftField.Requirement, thriftType: ThriftType): ThriftField =
    new ThriftField(name, 1, requirement, thriftType)

  def toThriftType(mani: Manifest[_], name: String, requirement: ThriftField.Requirement): ThriftType =
    if (manifest[Boolean] == mani)
      new ThriftType.BoolType
    else if (manifest[Byte] == mani)
      new ThriftType.ByteType
    else if (manifest[Double] == mani)
      new ThriftType.DoubleType
    else if (manifest[Short] == mani)
      new ThriftType.I16Type()
    else if (manifest[Int] == mani)
      new ThriftType.I32Type()
    else if (manifest[Long] == mani)
      new ThriftType.I64Type()
    else if (manifest[String] == mani)
      new ThriftType.StringType()
    else if (manifest[CMap[_, _]].runtimeClass == mani.runtimeClass) {
      val args       = mani.typeArguments
      val keyType    = toThriftType(args(0), name + "_map_key", requirement)
      val valueType  = toThriftType(args(1), name + "_map_value", requirement)
      val keyField   = fieldOf(name + "_map_key", requirement, keyType)
      val valueField = fieldOf(name + "_map_value", requirement, valueType)
      new ThriftType.MapType(keyField, valueField)
    } else if (manifest[Seq[_]].runtimeClass == mani.runtimeClass) {
        val elementType = toThriftType(mani.typeArguments.head, name, requirement)
        new ThriftType.ListType(fieldOf(name, requirement, elementType))
    } else
      convert(mani.runtimeClass.asInstanceOf[Class[_ <: ThriftStruct]])


  // see ScroogeMetaHelper
  def isUnionField(f: ThriftStructField[_]) = f.method == null

  def isOptional(f: ThriftStructField[_]): Boolean =
    isUnionField(f) ||
    // or fall back to using the method
    f.method.getReturnType() == classOf[Option[_]]

  def argsOf(field: ThriftStructField[_]): List[Manifest[_]] =
    field.manifest.get.typeArguments

  /** Gets the manifest for `T`. */
  def manifest[T : Manifest]: Manifest[T] = implicitly[Manifest[T]]

  /*
   * 'Ere be dragons. Scrooge generates two fields for an ENUM,
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
    fields.groupBy(_.tfield.id).values.toList.flatMap {
      case xs @ x :: y :: Nil => {
        val xType = ThriftTypeID.fromByte(x.tfield.`type`)
        val yType = ThriftTypeID.fromByte(y.tfield.`type`)

        if      (xType == ENUM && yType == I32)  List[ThriftStructField[_]](x)
        else if (xType == I32  && yType == ENUM) List[ThriftStructField[_]](y)
        else                                     xs
      }
      case xs => xs
    }.sortBy(_.tfield.id)
}
