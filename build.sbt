name := "Project3"

version := "0.1.0-SNAPSHOT"

scalaVersion := "2.12.8"
lazy val root = (project in file("."))
  .settings(
    name := "kafka-client"
  )

libraryDependencies += "org.apache.kafka" % "kafka-clients" % "2.3.1"
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.2.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.2.0"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "3.2.0"