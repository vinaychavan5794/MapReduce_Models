name := "CS441HW2"

version := "0.1"

scalaVersion := "2.13.3"


// Merge strategy to avoid deduplicate errors
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

libraryDependencies ++= Seq(
  "org.slf4j" % "slf4j-api" % "1.6.4",
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "com.typesafe" % "config" % "1.3.4",
  "org.scalatest" %% "scalatest" % "3.0.8" % "test",
  "junit" % "junit" % "4.11" % Test,
  "org.apache.hadoop" % "hadoop-client" % "2.4.0" exclude("org.slf4j", "slf4j-log4j12"),
  "org.apache.hadoop" % "hadoop-common" % "2.4.0" exclude("org.slf4j", "slf4j-log4j12"),
  "org.scala-lang.modules" %% "scala-xml" % "1.2.0"
)

// Set default main class for "sbt run" and "sbt assembly"
mainClass in (Compile, run) := Some("com.vchava2.MapRedDriver")
mainClass in assembly := Some("com.vchava2.MapRedDriver")
assemblyJarName in assembly := "vinay_indrajit_chavan_hw2.jar"


