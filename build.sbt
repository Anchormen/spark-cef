name := "spark-cef"
version := "0.6.23" // <major>.<minor>.<CEF-version>
organization := "nl.anchormen"
scalaVersion := "2.10.6"
val sparkVersion = "1.6.1"

resolvers += 
  "MapR" at "http://repository.mapr.com/maven/"

//	"com.fasterxml.jackson.core" % "jackson-databind" % "2.4.4",
//	"com.mapr.db" % "maprdb" % "5.2.0-mapr",
//	"com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.7.2",
libraryDependencies ++= Seq(
	"org.apache.spark" %% "spark-core" % sparkVersion % "provided",
	"org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
	"org.scalatest" %% "scalatest" % "2.2.1" % "test",
  	"com.novocode" % "junit-interface" % "0.9" % "test"
)
