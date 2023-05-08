ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "3.2.2"

lazy val root = (project in file("."))
  .settings(
    name := "minio1"
  )

val http4sVersion = "0.23.13"
val minioVersion = "8.5.2"
val catsVersion = "2.5.4"

libraryDependencies ++= Seq(
  "org.http4s" %% "http4s-dsl" % http4sVersion,
  "org.http4s" %% "http4s-blaze-server" % http4sVersion,
  "org.http4s" %% "http4s-blaze-client" % http4sVersion
)

libraryDependencies += "org.typelevel" %% "cats-effect" % catsVersion
ThisBuild / libraryDependencySchemes += "org.typelevel" %% "cats-effect" % "always"
libraryDependencies += "com.softwaremill.sttp.tapir" %% "tapir-http4s-server" % "1.2.9"
libraryDependencies += "com.softwaremill.sttp.client3" %% "core" % "3.8.12"
libraryDependencies += "com.softwaremill.sttp.tapir" %% "tapir-files" % "1.3.0"
// "com.softwaremill.sttp.client3" %% "upickle" % "3.8.12"

libraryDependencies += "io.minio" % "minio" % minioVersion

