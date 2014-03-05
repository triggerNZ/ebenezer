package au.com.cba.omnia.ebenezer
package introspect

import parquet.io.api.Binary
import parquet.io.api.GroupConverter
import parquet.schema.GroupType
import parquet.schema.OriginalType
import scala.collection.JavaConverters._
import scalaz._, Scalaz._

class IntrospectionListConverter(val schema: GroupType, done: List[Value] => Unit) extends GroupConverter {
  val values: scala.collection.mutable.ListBuffer[Value] = scala.collection.mutable.ListBuffer()
  val fields = schema.getFields.asScala.toList

  override def start = { }
  override def end = { done(values.toList) <| (_ => values.clear) }

  override def getConverter(n: Int) =
    converters(n)

  lazy val converters = fields.map({
    case field if field.isPrimitive && field.getOriginalType == OriginalType.ENUM =>
      new IntrospectionEnumConverter(values += _)
    case field if field.isPrimitive =>
      new IntrospectionPrimitiveConverter(values += _)
    case field if field.getOriginalType == OriginalType.LIST =>
      new IntrospectionListConverter(field.asGroupType, nested => values += ListValue(nested))
    case field if field.getOriginalType == OriginalType.MAP =>
      new IntrospectionMapConverter(field.asGroupType, value => values += MapValue(value))
    case field =>
      new IntrospectionRecordConverter(field.asGroupType, record => values += RecordValue(record))
  })

}
