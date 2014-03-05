package au.com.cba.omnia.ebenezer
package introspect

import java.util.{Map => JMap}

import org.apache.hadoop.conf.Configuration

import parquet.hadoop.api.InitContext
import parquet.hadoop.api.ReadSupport
import parquet.hadoop.api.ReadSupport._
import parquet.io.api.RecordMaterializer
import parquet.schema.MessageType

class IntrospectionReadSupport extends ReadSupport[Record] {
  override def prepareForRead(conf: Configuration, metaData: JMap[String, String], schema: MessageType, context: ReadContext) =
    new IntrospectionMaterializer(schema)

  override def init(context: InitContext): ReadContext =
    new ReadContext(context.getFileSchema)
}
