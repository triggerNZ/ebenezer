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

import sbt._, Keys._

import au.com.cba.omnia.uniform.core.standard.StandardProjectPlugin._
import au.com.cba.omnia.uniform.core.version.UniqueVersionPlugin._
import au.com.cba.omnia.uniform.dependency.UniformDependencyPlugin._
import au.com.cba.omnia.uniform.thrift.UniformThriftPlugin._
import au.com.cba.omnia.uniform.assembly.UniformAssemblyPlugin._

object build extends Build {
  lazy val standardSettings =
    Defaults.defaultSettings ++
    uniformDependencySettings ++
    uniform.docSettings("https://github.com/CommBank/ebenezer")

  lazy val all = Project(
    id = "all",
    base = file("."),
    settings =
      standardSettings
        ++ uniform.ghsettings
        ++ Seq(
          publishArtifact := false
        ),
    aggregate = Seq(core, hive)
  )

  lazy val core = Project(
    id = "core",
    base = file("core"),
    settings = 
      standardSettings
        ++ uniform.project("ebenezer", "au.com.cba.omnia.ebenezer")
        ++ uniformThriftSettings
        ++ Seq(
          libraryDependencies ++=
            depend.hadoop() ++ depend.scalding() ++ depend.scalaz() ++ depend.testing() ++ Seq(
              "com.twitter"             %  "parquet-cascading"  % "1.4.1",
              "au.com.cba.omnia"        %% "thermometer"        % "0.1.0-20140604034707-ce7a9d3" % "test"
            )
        )
  )

  lazy val hive = Project(
    id = "hive",
    base = file("hive"),
    settings =
      standardSettings
        ++ uniform.project("ebenezer-hive", "au.com.cba.omnia.ebenezer.hive")
        ++ uniformThriftSettings
        ++ Seq(
          libraryDependencies ++=
            depend.hadoop() ++ depend.scalding() ++ depend.testing() ++
            depend.omnia("cascading-hive", "1.2.0-20140721065812-4a3ea74") ++
            Seq(
              "au.com.cba.omnia"  %% "thermometer" % "0.1.0-20140604034707-ce7a9d3" % "test"
            )
        )
  ).dependsOn(core)

  lazy val tools = Project(
    id = "tools",
    base = file("tools"),
    settings =
      standardSettings
        ++ uniform.project("ebenezer-tools", "au.com.cba.omnia.ebenezer.cli")
        ++ uniformThriftSettings
        ++ uniformAssemblySettings
        ++ Seq(
          libraryDependencies ++=
            depend.hadoop() ++ depend.scalding() ++ depend.scalaz() ++ depend.testing()
        )
  ).dependsOn(core)

  lazy val example = Project(
    id = "example",
    base = file("example"),
    settings =
      standardSettings
        ++ uniform.project("ebenezer-example", "au.com.cba.omnia.ebenezer.example")
        ++ uniformThriftSettings
        ++ uniformAssemblySettings
        ++ Seq(
          libraryDependencies ++=
            depend.hadoop() ++ depend.scalding() ++ depend.testing()
        )
  ).dependsOn(hive)
}
