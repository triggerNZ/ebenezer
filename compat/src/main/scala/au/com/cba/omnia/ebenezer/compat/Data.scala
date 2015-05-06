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

package au.com.cba.omnia.ebenezer.compat

import scala.collection.mutable.ArrayBuffer

/** Data for compatibility tests */
object Data {
  val collection = List(
    Collection("", true, 0, 0, 0, 0.0, 0, Some(""), Some(0.0), List.empty, Map.empty),
    Collection("a", true, -1, -1, -1, -1.0, 1, Some("string a"), Some(4.0), List.empty, Map.empty),
    Collection("a b", false, 50, 10, 20, 30.0, 5, Some("string a"), Some(10.0), List.empty, Map.empty),
    Collection("a b c", false, Short.MaxValue, Int.MaxValue, Long.MaxValue, Double.MaxValue, Byte.MaxValue, Some("string a"), Some(Double.MaxValue), ArrayBuffer("a", "a b", "a b c"), Map("a" -> 1, "b" -> 3, "a b" -> 4)),
    Collection("a b c", false, Short.MinValue, Int.MinValue, Long.MinValue, Double.MinValue, Byte.MinValue, Some("string a"), Some(Double.MinValue), ArrayBuffer("a", "a b", "a b c"), Map("a" -> 1, "b" -> 3, "a b" -> 4))
  )

  val collectionTupled = collection.map(t => (t._1, t._2, t._3, t._4, t._5, t._6, t._7, t._8, t._9, t._10, t._11))
  val collectionTsv    = collection.map(_.productIterator.toList.mkString("|"))

  val simple = List(
    Simple("", true, 0, 0, 0, 0.0, 0, Some(""), Some(0.0)),
    Simple("a", true, -1, -1, -1, -1.0, 1, Some("string a"), Some(4.0)),
    Simple("a b", false, 50, 10, 20, 30.0, 5, Some("string a"), Some(10.0)),
    Simple("a b c", false, Short.MaxValue, Int.MaxValue, Long.MaxValue, Double.MaxValue, Byte.MaxValue, Some("string a"), Some(Double.MaxValue)),
    Simple("a b c", false, Short.MinValue, Int.MinValue, Long.MinValue, Double.MinValue, Byte.MinValue, Some("string a"), Some(Double.MinValue))
  )

  val simpleTupled = simple.map(t => (t._1, t._2, t._3, t._4, t._5, t._6, t._7, t._8, t._9))
  val simpleTsv    = simple.map(_.productIterator.toList.mkString("|"))

}
