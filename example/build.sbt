uniform.project("ebenezer-example", "au.com.cba.omnia.ebenezer.example")

uniformThriftSettings

uniformDependencySettings

uniformAssemblySettings

abjectJarSettings

libraryDependencies ++=
  depend.hadoop() ++ depend.scalding() ++ depend.scalaz() ++ depend.testing() ++ Seq(
    "au.com.cba.omnia"        %% "thermometer"        % "0.0.1-20140320004039-cf3a3f5"
  )
