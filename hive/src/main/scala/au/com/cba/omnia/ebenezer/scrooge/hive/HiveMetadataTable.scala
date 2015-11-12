//   Copyright 2015 Commonwealth Bank of Australia
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

package au.com.cba.omnia.ebenezer.scrooge.hive

import scala.collection.JavaConversions._
import scala.collection.{Map => CMap}

import org.apache.hadoop.fs.Path

import cascading.scheme.Scheme

import org.apache.hadoop.hive.metastore.TableType
import org.apache.hadoop.hive.metastore.api.{Table => MetadataTable, StorageDescriptor, FieldSchema}

import org.apache.thrift.protocol.TType

import com.twitter.scrooge.{ThriftStruct, ThriftStructCodec, ThriftStructField}

import au.com.cba.omnia.ebenezer.reflect.Reflect
import au.com.cba.omnia.ebenezer.scrooge.ParquetScroogeScheme

/** Replicates the functionality from cascading-hive.*/
object HiveMetadataTable {
  
  /** While the earlier operations failed with exception from with the cascading-hive code,
    * we need to deal with failure via `Result`
    */
  def apply[T <: ThriftStruct](
    database: String, 
    tableName: String,
    partitionColumns: List[(String, String)], 
    format: HiveStorageFormat,
    location: Option[Path] = None
  )(implicit m: Manifest[T]): MetadataTable = { // This operation could fail so type should convey it
    val thrift: Class[T]      = m.runtimeClass.asInstanceOf[Class[T]]
    val codec                 = Reflect.companionOf(thrift).asInstanceOf[ThriftStructCodec[_ <: ThriftStruct]]
    val metadata              = codec.metaData
    //Making the types lowercase to enforce the same behaviour as in cascading-hive
    val columnFieldSchemas    = metadata.fields.sortBy(_.id).map { c => 
      fieldSchema(c.name, mapType(c).toLowerCase)
    }
    val partitionFieldSchemas = partitionColumns.map {case(n, t) => fieldSchema(n, t)}

    assert(
      partitionColumns.map(_._1).intersect(metadata.fields.map(_.name)).isEmpty,
      "Partition columns must be different from the fields in the thrift struct"
    )

    val table = new MetadataTable();
    table.setDbName(database.toLowerCase); 
    table.setTableName(tableName.toLowerCase)

    val sd = new StorageDescriptor();
    columnFieldSchemas.foreach(f => sd.addToCols(f)) 

    location.fold(table.setTableType(TableType.MANAGED_TABLE.toString()))(p => {
      table.setTableType(TableType.EXTERNAL_TABLE.toString())
      //Copied from cascading-hive - Need to set this as well since setting the table type would be too obvious
      table.putToParameters("EXTERNAL", "TRUE");
      sd.setLocation(p.toString())
    })
    table.setSd(sd)

    if (!partitionFieldSchemas.isEmpty) {
        table.setPartitionKeys(partitionFieldSchemas)
        table.setPartitionKeysIsSet(true)
    }
    format.applyFormat(table)
  }

  def fieldSchema(n: String, t: String) =
    new FieldSchema(n, t, "Created by Ebenezer")

  /** Maps Thrift types to Hive types.*/
  def mapType(field: ThriftStructField[_]): String = {
    field.`type` match {
      case TType.BOOL   => "boolean"
      case TType.BYTE   => "tinyint"
      case TType.I16    => "smallint"
      case TType.I32    => "int"
      case TType.I64    => "bigint"
      case TType.DOUBLE => "double"
      case TType.STRING => "string"

      // 1 type param
      case TType.LIST   => {
        val elementType = toThriftType(argsOf(field).head)
        s"array<$elementType>"
      }
      case TType.SET    => throw new Exception("SET is not a supported Hive type")
      case TType.ENUM   => throw new Exception("ENUM is not a supported Hive type")

      // 2 type params
      case TType.MAP    => {
        val args      = argsOf(field)
        val keyType   = toThriftType(args(0))
        val valueType = toThriftType(args(1))
        s"map<$keyType,$valueType>"
      }

      // n type params
      case TType.STRUCT => throw new Exception("STRUCT is not a supported Hive type")

      // terminals
      case TType.VOID   => throw new Exception("VOID is not a supported Hive type")
      case TType.STOP   => throw new Exception("STOP is not a supported Hive type")
    }
  }

  /** Gets the manifests of the type arguments for complex thrift types such as Map. */
  def argsOf(field: ThriftStructField[_]): List[Manifest[_]] = {
    field.manifest.get.typeArguments
  }

  /** Maps manifest information to hive types. */
  def toThriftType(mani: Manifest[_]): String = {
    if (manifest[Boolean] == mani)
      "boolean"
    else if (manifest[Byte] == mani)
      "tinyint"
    else if (manifest[Double] == mani)
      "double"
    else if (manifest[Short] == mani)
      "smallint"
    else if (manifest[Int] == mani)
      "int"
    else if (manifest[Long] == mani)
      "bigint"
    else if (manifest[String] == mani)
      "string"
    else if (manifest[CMap[_, _]].runtimeClass == mani.runtimeClass) {
      val args      = mani.typeArguments
      val keyType   = toThriftType(args(0))
      val valueType = toThriftType(args(1))
      s"map<$keyType,$valueType>"
    } else if (manifest[Seq[_]].runtimeClass == mani.runtimeClass) {
      val elementType = toThriftType(mani.typeArguments.head)
      s"array<$elementType>"
    } else
      throw new Exception(s"$mani is not a supported nested type")
  }

  /** Gets the manifest for `T`. */
  def manifest[T : Manifest]: Manifest[T] = implicitly[Manifest[T]]
}