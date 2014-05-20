package au.com.cba.omnia.ebenezer.scrooge
package scalding

import com.twitter.scalding._
import java.util.UUID

abstract class UniqueJob(args: Args) extends Job(args) {
  lazy val unique: UUID = UUID.randomUUID

  override def name: String =
    super.name + "-" + unique.toString
}
