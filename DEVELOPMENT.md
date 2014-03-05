ebenezer
========

To build ebenezer, you need to add credentials for the commbank-releases artifactory
repository (whilst it depends on pre-open-sourced libraries).


To do this, add the following to your user sbt directory (i.e. `~/.sbt/<version>/commbank.sbt`):

```
credentials += Credentials("Artifactory Realm",
                           "commbank.artifactoryonline.com",
                           "<your artifactory username>",
                           "<your artifactory password>")
```

speeding things up
------------------

If you want to depend on thermometer as source, copy `project/source.scala.source-dependencies` to `project/source.scala`. `source.scala` is in `.gitignore` so it can not be accidently checked in.
