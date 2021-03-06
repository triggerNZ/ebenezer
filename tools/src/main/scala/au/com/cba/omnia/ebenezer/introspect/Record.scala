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
