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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import parquet.hadoop.ParquetReader

object ParquetIntrospectTools {
  def iteratorFromPath(conf: Configuration, path: Path): Iterator[Record] = {
    val reader = new ParquetReader[Record](conf, path, new IntrospectionReadSupport)
    new Iterator[Record] {
      var state: Record = reader.read
      def hasNext = state != null
      def next = { val record = state; state = reader.read; record }
    }
  }

  def streamFromPath(conf: Configuration, path: Path): Stream[Record] =
    iteratorFromPath(conf, path).toStream

  def listFromPath(conf: Configuration, path: Path): List[Record] =
    iteratorFromPath(conf, path).toList
}
