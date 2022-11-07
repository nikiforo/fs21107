ThisBuild / scalaVersion := "2.13.8"
ThisBuild / organization := "ru.delimobil"

name := "fs-example"

libraryDependencies ++=
  Seq(
    "co.fs2" %% "fs2-core" % "3.3.0"
  )
