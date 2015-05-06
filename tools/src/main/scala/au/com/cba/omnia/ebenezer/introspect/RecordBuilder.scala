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

package au.com.cba.omnia.ebenezer.introspect

/**
 * This is a *very* unsafe builder for untyped records. It will accumulate calls until a toRecord call, at
 * which time it will construct an immutable record and reset itself to continue building the next record.
 */
case class RecordBuilder(data: scala.collection.mutable.ListBuffer[Field] = scala.collection.mutable.ListBuffer()) {
  def add(name: String, value: Value) =
    data.append(Field(name, value))

  def toRecord: Record =
    Record(data.toList)

  def clear() =
    data.clear
}
