uniform.project("ebenezer-example1", "au.com.cba.omnia.ebenezer.example1")

uniformThriftSettings

uniformDependencySettings

uniformAssemblySettings

abjectJarSettings

javacOptions ++= Seq("-source", "1.6", "-target", "1.6")

scalacOptions ++= Seq("-target:jvm-1.6")

libraryDependencies ++=
  depend.hadoop() ++ depend.scalding() ++ depend.scalaz() ++ depend.testing() ++ Seq(
    "au.com.cba.omnia"        %% "thermometer"        % "0.0.1-20140320004039-cf3a3f5"
  )
