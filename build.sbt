name := "cloudwatch-logback-appender"

version := "0.0.1-SNAPSHOT"

autoScalaLibrary := false

libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.4.5"

libraryDependencies ++= Seq("logs", "ec2").map(service => "com.amazonaws" % s"aws-java-sdk-$service" % "1.11.914")

libraryDependencies += "com.github.sbt" % "junit-interface" % "0.13.3" % Test
libraryDependencies += "org.easymock" % "easymock" % "5.2.0" % Test
