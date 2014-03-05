import java.io.File
import java.text.SimpleDateFormat
import java.util.{Date, TimeZone}

import sbt._, Keys._

object UniqueVersion extends Plugin {
  def uniqueVersionSettings = Seq(
    version in ThisBuild        <<=  (version in ThisBuild, baseDirectory).apply((v, d)=> v + "-" + timestamp(now) + "-" + commish(d))
  )

  def timestamp(instant: Date, format: String = "yyyyMMddHHmmss") = {
    val formatter = new SimpleDateFormat("yyyyMMddHHmmss")
    formatter.setTimeZone(TimeZone.getTimeZone("UTC"))
    formatter.format(instant)
  }

  def commish(root: File) =
    gitlog(root, "%h")

  def commit(root: File) =
    gitlog(root, "%H")

  def gitlog(root: File, format: String) =
    Process("git log --pretty=format:" + format + " -n  1", Some(root)).lines.head

  lazy val now =
    new Date
}
