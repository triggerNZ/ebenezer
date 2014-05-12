uniform.project("ebenezer-example", "au.com.cba.omnia.ebenezer.example")

uniformThriftSettings

uniformDependencySettings

uniformAssemblySettings

abjectJarSettings

scalacOptions += "-target:jvm-1.6"

libraryDependencies ++=
  depend.hadoop() ++ depend.scalding() ++ depend.scalaz() ++ depend.testing() ++
  depend.omnia("cascading-beehaus", "0.1.0-20140507040900-76cbd44")

