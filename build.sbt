organization := "com.cba.omnia"

name := "ebenezer"

scalaVersion := "2.10.3"

scalacOptions := Seq(
  "-deprecation"
, "-unchecked"
, "-optimise"
, "-Ywarn-all"
, "-Xlint"
, "-Xfatal-warnings"
, "-feature"
, "-language:_"
)

com.twitter.scrooge.ScroogeSBT.newSettings
