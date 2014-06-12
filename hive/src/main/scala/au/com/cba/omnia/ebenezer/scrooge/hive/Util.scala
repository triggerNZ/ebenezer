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
package hive

import org.apache.thrift.protocol.TType

import cascading.tuple.Fields

import com.twitter.scalding.{Dsl, TupleConverter}

object Util {
  /*
   DO NOT USE intFields, scalding / cascading Fields.merge is broken and gets called in bowels of
   TemplateTap. See scalding/#803.
   */
  def toFields(start: Int, end: Int): Fields =
    Dsl.strFields((start until end).map(_.toString))

  def converter[A, T, U >: (A, T)](valueConverter: TupleConverter[T], partitionConverter: TupleConverter[A]) =
    TupleConverter.asSuperConverter[(A, T), U](new TupleConverter[(A, T)] {
      import cascading.tuple.TupleEntry

      def arity = valueConverter.arity + partitionConverter.arity

      def apply(te : TupleEntry) : (A, T) = {
        val value = new TupleEntry(toFields(0, valueConverter.arity))
        val partition = new TupleEntry(toFields(0, partitionConverter.arity))
          (0 until valueConverter.arity).foreach(idx => value.setObject(idx, te.getObject(idx)))
          (0 until partitionConverter.arity).foreach(idx => partition.setObject(idx, te.getObject(idx + valueConverter.arity)))
        (partitionConverter(partition), valueConverter(value))
      }
    })

  // TODO: complex type handling
  def mapType(thriftType: Byte): String = thriftType match {
    case TType.BOOL   => "boolean"
    case TType.BYTE   => "tinyint"
    case TType.I16    => "smallint"
    case TType.I32    => "int"
    case TType.I64    => "bigint"
    case TType.DOUBLE => "double"
    case TType.STRING => "string"
      
    // 1 type param
    case TType.LIST   => throw new Exception("LIST is not a supported Hive type")
    case TType.SET    => throw new Exception("SET is not a supported Hive type")
    case TType.ENUM   => throw new Exception("ENUM is not a supported Hive type")
      
    // 2 type params
    case TType.MAP    => throw new Exception("MAP is not a supported Hive type")
      
    // n type params
    case TType.STRUCT => throw new Exception("STRUCT is not a supported Hive type")
      
    // terminals
    case TType.VOID   => throw new Exception("VOID is not a supported Hive type")
    case TType.STOP   => throw new Exception("STOP is not a supported Hive type")
  }
}

