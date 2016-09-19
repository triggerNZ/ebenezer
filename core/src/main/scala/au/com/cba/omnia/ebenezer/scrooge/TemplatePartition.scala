//   Copyright 2015 Commonwealth Bank of Australia
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
package scrooge

import scala.collection.JavaConverters._

import cascading.tuple.{Fields, TupleEntry}
import cascading.tap.partition.Partition

/** Creates a partition using the given template string. The template string needs to have %s as placeholder for a given field. */
case class TemplatePartition(partitionFields: Fields, template: String) extends Partition {
  assert(partitionFields.size == "%s".r.findAllMatchIn(template).length)

  lazy val pattern = template.replaceAll("%s", "(.*)").r.pattern

  override def getPathDepth(): Int = partitionFields.size

  override def getPartitionFields(): Fields = partitionFields

  override def toTuple(partition: String, tupleEntry: TupleEntry): Unit = {
    val m = pattern.matcher(partition)
    m.matches
    val parts: Array[Object] = (1 to partitionFields.size).map(i => m.group(i)).toArray
    tupleEntry.setCanonicalValues(parts)
  }

  override def toPartition(tupleEntry: TupleEntry): String = {
    val fields = tupleEntry.asIterableOf(classOf[String]).asScala.toList
    template.format(fields: _*)
  }
}
