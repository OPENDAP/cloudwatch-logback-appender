name := "cloudwatch-logback-appender"

version := "0.0.1-SNAPSHOT"

autoScalaLibrary := false

libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.5.6"

libraryDependencies ++= Seq("cloudwatchlogs", "ec2", "imds").map(service => "software.amazon.awssdk" % service % "2.25.60")

libraryDependencies += "com.github.sbt" % "junit-interface" % "0.13.3" % Test
libraryDependencies += "org.easymock" % "easymock" % "5.2.0" % Test

fork := true