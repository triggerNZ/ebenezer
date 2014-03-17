uniform.project("ebenezer", "au.com.cba.omnia.ebenezer")

uniformThriftSettings

uniformDependencySettings

libraryDependencies ++=
  depend.hadoop() ++ depend.scalding() ++ depend.scalaz() ++ depend.testing() ++ Seq(
    "com.twitter"             %  "parquet-cascading"  % "1.3.2",
    "au.com.cba.omnia"        %% "thermometer"        % "0.0.1-20140317094330-d6d48e2" % "test"
  )
