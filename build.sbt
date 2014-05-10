name := "akka-esper-integration"

version := "1.0"

scalaVersion := "2.10.3"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % "2.2.3",
  "com.espertech" % "esper" % "4.11.0",
  "com.gensler" %% "scalavro-util" % "0.6.2",
  "com.typesafe.akka" %% "akka-stream-experimental" % "0.2"
)