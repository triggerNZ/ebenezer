resolvers += Resolver.url("commbank-releases-ivy", new URL("http://commbank.artifactoryonline.com/commbank/ext-releases-local-ivy"))(Patterns("[organization]/[module]_[scalaVersion]_[sbtVersion]/[revision]/[artifact](-[classifier])-[revision].[ext]"))

addSbtPlugin("au.com.cba.omnia" % "uniform-core" % "0.0.1-20140306044712-c3a56bf")

addSbtPlugin("au.com.cba.omnia" % "uniform-dependency" % "0.0.1-20140306044712-c3a56bf")

addSbtPlugin("au.com.cba.omnia" % "uniform-thrift" % "0.0.1-20140306044712-c3a56bf")
