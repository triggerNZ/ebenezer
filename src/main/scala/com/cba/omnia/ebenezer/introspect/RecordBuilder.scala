package com.cba.omnia.ebenezer
package introspect

import scalaz._, Scalaz._

/**
 * This is a *very* unsafe builder for untyped records. It will accumulate calls until a toRecord call, at
 * which time it will construct an immutable record and reset itself to continue building the next record.
 */
case class RecordBuilder(data: scala.collection.mutable.ListBuffer[Field] = scala.collection.mutable.ListBuffer()) {
  def add(name: String, value: Value) =
    data += Field(name, value)

  def toRecord: Record =
    Record(data.toList)

  def clear() =
    data.clear
}
