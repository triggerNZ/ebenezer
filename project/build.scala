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

import sbt._
import Keys._


import au.com.cba.omnia.uniform.core.standard.StandardProjectPlugin._
import au.com.cba.omnia.uniform.core.version.UniqueVersionPlugin._
import au.com.cba.omnia.uniform.dependency.UniformDependencyPlugin._

object build extends Build {
  type Sett = Project.Setting[_]

  lazy val core = project.in(file("core"))
  lazy val hive = project.in(file("hive"))
                    .dependsOn(core)
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
