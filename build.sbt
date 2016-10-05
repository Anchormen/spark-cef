name := "spark-cef"
version := "0.5.23" // <major>.<minor>.<CEF-version>
organization := "nl.anchormen"
scalaVersion := "2.10.5"
val sparkVersion = "1.6.1"

libraryDependencies ++= Seq(
	"org.apache.hbase" % "hbase-common" % "1.2.3",
	"org.apache.hbase" % "hbase-client" % "1.2.3",
	"org.apache.spark" %% "spark-core" % sparkVersion % "provided",
	"org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
	"org.scalatest" %% "scalatest" % "2.2.1" % "test",
  	"com.novocode" % "junit-interface" % "0.9" % "test"
)
