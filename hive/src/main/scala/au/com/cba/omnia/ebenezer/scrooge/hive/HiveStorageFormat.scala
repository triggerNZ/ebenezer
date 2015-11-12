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

package au.com.cba.omnia.ebenezer.scrooge.hive

import scala.collection.JavaConversions._

import org.apache.hadoop.hive.metastore.api.{Table => MetadataTable}
import org.apache.hadoop.hive.metastore.api.SerDeInfo

/** Data type used to indicate Input and Output format for Hive table */
trait HiveStorageFormat {
  def inputFormat: String
  def outputFormat: String
  def serializationLibrary: String

  def applyFormat(table: MetadataTable): MetadataTable = {
    val serDeInfo = new SerDeInfo()
    serDeInfo.setSerializationLib(serializationLibrary)
    serDeInfo.setParameters(Map("serialization.format" -> "1"))
    
    table.getSd().setSerdeInfo(serDeInfo);
    table.getSd().setInputFormat(inputFormat)
    table.getSd().setOutputFormat(outputFormat)
    table
  }
}

case object ParquetFormat extends HiveStorageFormat {
  val inputFormat          = classOf[org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat].getName
  val outputFormat         = classOf[org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat].getName
  val serializationLibrary = classOf[org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe].getName
}

case class TextFormat(delimiter: String = TextFormat.DEFAULT_DELIMITER) extends HiveStorageFormat {
  val inputFormat          = classOf[org.apache.hadoop.mapred.TextInputFormat].getName
  val outputFormat         = classOf[org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat[_, _]].getName
  val serializationLibrary = classOf[org.apache.hadoop.hive.serde2.`lazy`.LazySimpleSerDe].getName
  
  override def applyFormat(table: MetadataTable): MetadataTable = {
    val hiveMetaDataTable = super.applyFormat(table)
    hiveMetaDataTable.getSd().getSerdeInfo().setParameters(
      Map("serialization.format" -> delimiter, "field.delim" -> delimiter)
    )
    hiveMetaDataTable
  }
}

object TextFormat {
  val DEFAULT_DELIMITER = "\u0001"
}
