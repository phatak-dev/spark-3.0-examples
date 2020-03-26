name := "spark-three-examples"

version := "0.1"

scalaVersion := "2.12.4"

val sparkVersion = "3.0.0-SNAPSHOT"


resolvers += "Spark Snapshot Repository" at "https://repository.apache.org/snapshots"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "mysql" % "mysql-connector-java" % "5.1.6"
)

