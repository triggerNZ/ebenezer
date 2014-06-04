uniform.project("ebenezer-hive", "au.com.cba.omnia.ebenezer.scrooge.hive")

uniformThriftSettings

uniformDependencySettings

libraryDependencies ++=
  depend.hadoop() ++ depend.scalding() ++ depend.scalaz() ++ depend.testing() ++ 
depend.omnia("cascading-beehaus", "0.2.0-20140604033439-6dbe4c7") ++
  Seq(
    "au.com.cba.omnia"        %% "thermometer"        % "0.1.0-20140604034707-ce7a9d3" % "test"
  )
