import sbt._
import Keys._

object source extends Build {
  lazy val ref =
    Project("root", file("."))
      .dependsOn(file("../thermometer"))
}
