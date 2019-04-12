name := "spark2"

version := "0.1"

scalaVersion := "2.12.8"
organization := "ch.epfl.scala"


libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.1"

libraryDependencies += "org.typelevel" %% "cats-core" % "1.6.0"
libraryDependencies += "org.apache.kafka"%"kafka-clients"%"2.1.0"
libraryDependencies += "com.typesafe.play"%%"play-json"% "2.7.2"