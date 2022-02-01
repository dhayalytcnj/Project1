ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.13"

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.1.2"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.1.2"

libraryDependencies += "org.apache.spark" %% "spark-hive" % "3.1.2"

lazy val root = (project in file("."))
  .settings(
    name := "Project1"
  )
