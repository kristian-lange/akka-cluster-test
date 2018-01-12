name := "akka-distributed-scrapers"

version := "1.0"

scalaVersion := "2.11.11"
lazy val akkaVersion = "2.5.4"

fork in Test := true

resolvers += DefaultMavenRepository

unmanagedBase <<= baseDirectory { base => base / "../scraper-scripts/build/libs" }

libraryDependencies ++= Seq(
  "org.codehaus.groovy"  %"groovy-all"                         % "2.4.5",
  "com.talentwunder"  %  "scraper-scripts"                     % "1.0.0",

  "com.typesafe.akka" %% "akka-cluster"                        % akkaVersion,
  "com.typesafe.akka" %% "akka-cluster-tools"                  % akkaVersion,

  "com.typesafe.akka" %% "akka-slf4j"                          % akkaVersion,
  "ch.qos.logback"    %  "logback-classic"                     % "1.2.3",

  // test dependencies
  "com.typesafe.akka" %% "akka-testkit"                        % akkaVersion              % "test",
  "org.scalatest"     %% "scalatest"                           % "3.0.1"                  % "test",
  "commons-io"        %  "commons-io"                          % "2.4"                    % "test")
