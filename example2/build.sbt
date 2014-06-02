uniform.project("ebenezer-example2", "au.com.cba.omnia.ebenezer.example2")

uniformThriftSettings

uniformDependencySettings

uniformAssemblySettings

javacOptions ++= Seq("-source", "1.6", "-target", "1.6")

scalacOptions ++= Seq("-target:jvm-1.6")

libraryDependencies ++=
  depend.hadoop() ++ depend.scalding() ++ depend.scalaz() ++ depend.testing() ++ Seq(
    "au.com.cba.omnia"        %% "thermometer"        % "0.0.1-20140320004039-cf3a3f5"
  )
