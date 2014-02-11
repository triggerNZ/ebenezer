package com.cba.omnia.ebenezer
package introspect

import parquet.io.api.Binary
import parquet.io.api.PrimitiveConverter

class IntrospectionPrimitiveConverter(name: String, builder: RecordBuilder) extends PrimitiveConverter {
  override def addBinary(value: Binary) =
    builder.add(name, StringValue(value.toStringUsingUTF8))

  override def addBoolean(value: Boolean) =
    builder.add(name, BooleanValue(value))

  override def addDouble(value: Double) =
    builder.add(name, DoubleValue(value))

  override def addFloat(value: Float) =
    builder.add(name, FloatValue(value))

  override def addInt(value: Int) =
    builder.add(name, IntValue(value))

  override def addLong(value: Long) =
    builder.add(name, LongValue(value))
}
