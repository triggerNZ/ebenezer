package com.cba.omnia.ebenezer
package introspect

case class Record(data: List[Field]) {
  override def toString = "Record[ " + data.mkString(" , ") + " ]"
}

case class Field(name: String, value: Value) {
  override def toString = name.toString + ": " + value.toString
}

sealed trait Value
case class RecordValue(v: Record) extends Value { override def toString = v.toString }
case class StringValue(v: String) extends Value { override def toString = '"' + v.toString + '"' }
case class IntValue(v: Int) extends Value { override def toString = v.toString }
case class FloatValue(v: Float) extends Value { override def toString = v.toString + "f" }
case class DoubleValue(v: Double) extends Value { override def toString = v.toString + "d" }
case class LongValue(v: Long) extends Value { override def toString = v.toString + "l" }
case class BooleanValue(v: Boolean) extends Value { override def toString = v.toString }
case class EnumValue(v: String) extends Value { override def toString = "<" + v.toString + ">" }
case class ListValue(vs: List[Value]) extends Value { override def toString =  "[" + vs.mkString(", ") + "]" }
case class MapValue(vs: Map[Value, Value]) extends Value { override def toString =  "{" + vs.toList.map({ case (k, v) => k + " -> " + v }).mkString(", ") + "}" }
