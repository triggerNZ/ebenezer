import sbt._
import Keys._


import au.com.cba.omnia.uniform.core.standard.StandardProjectPlugin._
import au.com.cba.omnia.uniform.core.version.UniqueVersionPlugin._
import au.com.cba.omnia.uniform.dependency.UniformDependencyPlugin._

object build extends Build {
  type Sett = Project.Setting[_]

  lazy val core = project.in(file("core"))
  lazy val hive = project.in(file("hive")).dependsOn(core)
  lazy val example = project.in(file("example")).dependsOn(hive)

  lazy val all = Project(
    id = "all",
    base = file("."),
    settings = Seq[Sett](
      publishArtifact := false
    ),
    aggregate = Seq(core, hive)
  )
}
