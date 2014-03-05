libraryDependencies ++= Seq(
  "com.twitter"             %% "scalding-core"      % "0.9.0rc4"
, "com.twitter"             %  "parquet-cascading"  % "1.3.2"
, "com.twitter"             %% "scrooge-core"       % "3.12.0"
, "com.twitter"             %% "scrooge-runtime"    % "3.12.0"
, "org.apache.thrift"       %  "libthrift"          % "0.8.0"
, "org.apache.hadoop"       %  "hadoop-client"      % "2.0.0-mr1-cdh4.3.0"  % "provided"
, "org.apache.hadoop"       %  "hadoop-core"        % "2.0.0-mr1-cdh4.3.0"  % "provided"
, "org.scalaz"              %% "scalaz-core"        % "7.0.5"
)

libraryDependencies ++= Seq(
  "com.cba.omnia"           %% "thermometer"        % "0.0.1-20140305011349-efe2e0f" % "test"
, "org.specs2"              %% "specs2"             % "2.2.2"                        % "test"
, "org.scalacheck"          %% "scalacheck"         % "1.10.1"                       % "test"
)

resolvers ++= Seq(
  "snapshots" at "http://oss.sonatype.org/content/repositories/snapshots"
, "releases" at "http://oss.sonatype.org/content/repositories/releases"
, "Concurrent Maven Repo" at "http://conjars.org/repo"
, "Clojars Repository" at "http://clojars.org/repo"
, "Twitter Maven" at "http://maven.twttr.com"
, "Hadoop Releases" at "https://repository.cloudera.com/content/repositories/releases/"
, "cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos/"
, "commbank-releases" at "http://commbank.artifactoryonline.com/commbank/ext-releases-local"
)
