name := "kafka-streams-poc"

version := "1.0"

scalaVersion := "2.12.2"

libraryDependencies += "org.json4s" %% "json4s-native" % "3.5.2"
libraryDependencies += "org.slf4j" % "slf4j-log4j12" % "1.7.25"
libraryDependencies += "org.apache.kafka" % "kafka-streams" % "0.10.2.1"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"
libraryDependencies += "net.manub" %% "scalatest-embedded-kafka" % "0.13.1" % "test"
libraryDependencies += "net.manub" %% "scalatest-embedded-kafka-streams" % "0.13.1" % "test"
