uniform.project("ebenezer-hive", "au.com.cba.omnia.ebenezer.hive")

uniformThriftSettings

uniformDependencySettings

uniformAssemblySettings

libraryDependencies ++=
  depend.hadoop() ++ depend.scalding() ++ depend.scalaz() ++ depend.testing() ++ 
  depend.omnia("cascading-beehaus", "0.0.1-20140414053827-dc68842") ++
  Seq(
    "au.com.cba.omnia"        %% "thermometer"        % "0.0.1-20140320004039-cf3a3f5" % "test"
  )

