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

uniform.project("ebenezer-hive", "au.com.cba.omnia.ebenezer.scrooge.hive")

uniformThriftSettings

uniformDependencySettings

libraryDependencies ++=
  depend.hadoop() ++ depend.scalding() ++ depend.scalaz() ++ depend.testing() ++ 
depend.omnia("cascading-beehaus", "0.2.0-20140604033439-6dbe4c7") ++
  Seq(
    "au.com.cba.omnia"        %% "thermometer"        % "0.1.0-20140604034707-ce7a9d3" % "test"
  )
