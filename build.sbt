Global / onChangedBuildSource := ReloadOnSourceChanges

scalaVersion := "2.13.8"

lazy val V = new {
  val skunk = "0.2.3"
  val circe = "0.14.1"
  val munit = "0.7.29"
  val munitCatsEffect3 = "1.0.7"
}

libraryDependencies ++= Seq(
  "org.tpolecat" %% "skunk-circe" % V.skunk,
  "io.circe" %% "circe-generic" % V.circe,
  "org.scalameta" %% "munit" % V.munit % Test,
  "org.typelevel" %% "munit-cats-effect-3" % V.munitCatsEffect3 % Test,
)
