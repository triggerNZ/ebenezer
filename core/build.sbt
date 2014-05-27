uniform.project("ebenezer", "au.com.cba.omnia.ebenezer")

uniformThriftSettings

uniformDependencySettings

uniformAssemblySettings

libraryDependencies ++=
  depend.hadoop() ++ depend.scalding() ++ depend.scalaz() ++ depend.testing() ++ Seq(
    "com.twitter"             %  "parquet-cascading"                  % "1.3.2",
    "au.com.cba.omnia"        %% "thermometer"        % "0.0.1-20140320004039-cf3a3f5" % "test"
  )
