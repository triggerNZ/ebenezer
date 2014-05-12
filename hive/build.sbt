uniform.project("ebenezer-hive", "au.com.cba.omnia.ebenezer.scrooge.hive")

uniformThriftSettings

uniformDependencySettings

libraryDependencies ++=
  depend.hadoop() ++ depend.scalding() ++ depend.scalaz() ++ depend.testing() ++ 
depend.omnia("cascading-beehaus", "0.1.0-20140507040900-76cbd44") ++
  Seq(
    "au.com.cba.omnia"        %% "thermometer"        % "0.0.1-20140320004039-cf3a3f5" % "test"
  )

