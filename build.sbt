name := "delta-uc-creds-test"

version := "0.1.0"

scalaVersion := "2.13.16"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "4.0.2-SNAPSHOT",
  "io.delta" %% "delta-spark" % "4.0.0",
  "org.apache.hadoop" % "hadoop-aws" % "3.4.1",
  "com.softwaremill.sttp.client3" %% "core" % "3.9.0",
  "io.circe" %% "circe-parser" % "0.14.6",
  "io.circe" %% "circe-generic" % "0.14.6"
)

resolvers += Resolver.mavenLocal
