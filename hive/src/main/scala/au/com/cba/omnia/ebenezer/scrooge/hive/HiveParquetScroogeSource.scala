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

import org.apache.hadoop.hive.conf.HiveConf

import cascading.scheme.Scheme
import cascading.tap.{Tap, SinkMode}

import cascading.tap.hive.HiveTap

import com.twitter.scalding._

import com.twitter.scrooge.{ThriftStruct, ThriftStructCodec}

import au.com.cba.omnia.beehaus.ParquetTableDescriptor

import au.com.cba.omnia.ebenezer.reflect.Reflect

case class HiveParquetScroogeSource[T <: ThriftStruct](database: String, table: String, conf: HiveConf)(implicit m : Manifest[T], conv: TupleConverter[T], set: TupleSetter[T])
  extends Source
  with TypedSink[T]
  with Mappable[T]
  with java.io.Serializable {

  lazy val thrift: Class[T] = manifest.runtimeClass.asInstanceOf[Class[T]]
  lazy val codec            = Reflect.companionOf(thrift).asInstanceOf[ThriftStructCodec[_ <: ThriftStruct]]
  lazy val metadata         = codec.metaData
  lazy val columns          = metadata.fields.map(_.name).toArray
  lazy val types            = metadata.fields.map(t => Util.mapType(t.`type`)).toArray

  lazy val tableDescriptor = new ParquetTableDescriptor(database, table, columns, types, Array())

  lazy val  hdfsScheme = HadoopSchemeInstance(new ParquetScroogeScheme[T].asInstanceOf[Scheme[_, _, _, _, _]])

  override def createTap(readOrWrite: AccessMode)(implicit mode: Mode): Tap[_, _, _] = mode match {
    case Local(_)              => sys.error("Local mode is currently not supported for ${toString}")
    case hdfsMode @ Hdfs(_, jobConf) => readOrWrite match {
      case Read  => CastHfsTap(new HiveTap(tableDescriptor, hdfsScheme, SinkMode.REPLACE, true))
      case Write => {
        val tap = new HiveTap(tableDescriptor, hdfsScheme, SinkMode.REPLACE, true)
        CastHfsTap(tap)
      }
    }
    case x                     => sys.error(s"$x mode is currently not supported for ${toString}")
  }

  override def toString: String = s"HiveParquetScroogeSource[${metadata.structName}]($database, $table)"

  override def converter[U >: T] =
    TupleConverter.asSuperConverter[T, U](conv)

  override def setter[U <: T] =
    TupleSetter.asSubSetter[T, U](TupleSetter.of[T])
}
