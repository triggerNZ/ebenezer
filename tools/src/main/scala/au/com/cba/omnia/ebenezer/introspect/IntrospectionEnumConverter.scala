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
