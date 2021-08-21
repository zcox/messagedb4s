Global / onChangedBuildSource := ReloadOnSourceChanges

scalaVersion := "2.13.6"

libraryDependencies ++= Seq(
  "org.tpolecat" %% "skunk-circe" % "0.2.2",
  "org.scalameta" %% "munit" % "0.7.27" % Test,
  "org.typelevel" %% "munit-cats-effect-3" % "1.0.0" % Test,
)
