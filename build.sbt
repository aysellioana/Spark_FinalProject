
ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.12"

lazy val root = (project in file("."))
  .settings(
    name := "Spark_FinalProject_1"
  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.2.0" withSources() withJavadoc(),
  "org.apache.spark" %% "spark-sql" % "3.2.0" withSources() withJavadoc(),
  "org.apache.spark" %% "spark-mllib" % "3.2.0" withSources() withJavadoc(),
  "org.apache.spark" %% "spark-streaming" % "3.2.0" withSources() withJavadoc(),
  "org.twitter4j" % "twitter4j-core" % "4.0.7" withSources() withJavadoc(),
  "org.twitter4j" % "twitter4j-stream" % "4.0.7" withSources() withJavadoc()
)