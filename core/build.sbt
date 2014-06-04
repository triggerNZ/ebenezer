uniform.project("ebenezer", "au.com.cba.omnia.ebenezer")

uniformThriftSettings

uniformDependencySettings

uniformAssemblySettings

libraryDependencies ++=
  depend.hadoop() ++ depend.scalding() ++ depend.scalaz() ++ depend.testing() ++ Seq(
    "com.twitter"             %  "parquet-cascading"  % "1.4.1",
    "au.com.cba.omnia"        %% "thermometer"        % "0.1.0-20140604034707-ce7a9d3" % "test"
  )
