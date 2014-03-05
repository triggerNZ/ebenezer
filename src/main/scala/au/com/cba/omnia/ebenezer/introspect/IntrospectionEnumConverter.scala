package au.com.cba.omnia.ebenezer
package introspect

import parquet.io.api.Binary
import parquet.io.api.PrimitiveConverter

class IntrospectionEnumConverter(done: Value => Unit) extends PrimitiveConverter {
  override def addBinary(value: Binary) =
    done(EnumValue(value.toStringUsingUTF8))

  override def addBoolean(value: Boolean) =
    sys.error(s"Did not expect boolean <$value> for enum.")

  override def addDouble(value: Double) =
    sys.error(s"Did not expect double <$value> for enum.")

  override def addFloat(value: Float) =
    sys.error(s"Did not expect float <$value> for enum.")

  override def addInt(value: Int) =
    sys.error(s"Did not expect int <$value> for enum.")

  override def addLong(value: Long) =
    sys.error(s"Did not expect long <$value> for enum.")
}
