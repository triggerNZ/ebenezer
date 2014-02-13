package com.cba.omnia.ebenezer
package introspect

import parquet.io.api.Binary
import parquet.io.api.PrimitiveConverter

class IntrospectionPrimitiveConverter(done: Value => Unit) extends PrimitiveConverter {
  override def addBinary(value: Binary) =
    done(StringValue(value.toStringUsingUTF8))

  override def addBoolean(value: Boolean) =
    done(BooleanValue(value))

  override def addDouble(value: Double) =
    done(DoubleValue(value))

  override def addFloat(value: Float) =
    done(FloatValue(value))

  override def addInt(value: Int) =
    done(IntValue(value))

  override def addLong(value: Long) =
    done(LongValue(value))
}
