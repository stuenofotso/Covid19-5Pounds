name := "ScalaSparkForecaster_h_covid19"

version := "0.1"

scalaVersion := "2.11.8"

scalacOptions ++= Seq("-deprecation")

resolvers += Resolver.sonatypeRepo("releases")

// grading libraries
libraryDependencies += "junit" % "junit" % "4.10" % Test
libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.0.5",
  "org.apache.spark" %% "spark-core" % "2.4.0",
  "org.apache.spark" %% "spark-sql" % "2.1.0",
  "org.apache.spark" %% "spark-mllib" % "2.4.0",
  "com.sksamuel.elastic4s" 	%% "elastic4s-core"    	  % "5.6.7" exclude("org.elasticsearch", "elasticsearch"),
  "org.elasticsearch" 		% "elasticsearch"           % "7.6.2",
  "org.elasticsearch" 		%% "elasticsearch-spark-20"    % "7.6.2"  excludeAll ExclusionRule(organization = "org.apache.spark"),
  "com.typesafe"      		% "config"                  % "1.2.1",
  "com.cloudera.sparkts" % "sparkts"                 % "0.4.0"
)



