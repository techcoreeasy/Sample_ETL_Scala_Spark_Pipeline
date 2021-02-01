name := "service-text-classification-data-processing-pipeline"

version := "0.1"

scalaVersion := "2.11.12"


val sparkVersion = "2.4.0"
val versionTypesafeConfig = "1.3.3"

dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core" % "2.8.7"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.8.7"
dependencyOverrides += "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.8.7"


libraryDependencies += "org.scala-lang" % "scala-library" % "2.11.12"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion
)


libraryDependencies += "org.scalatest" % "scalatest_2.11" % "3.0.5" % "test"
libraryDependencies += "com.github.mrpowers" % "spark-fast-tests" % "v0.16.0" % "test"

artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
  artifact.name + "_" + sv.binary + "-" + sparkVersion + "_" + module.revision + "." + artifact.extension
}

libraryDependencies += "com.amazonaws" % "aws-java-sdk" % "1.11.496"
libraryDependencies += "com.amazonaws" % "aws-java-sdk-core" % "1.11.496"
libraryDependencies += "com.amazonaws" % "aws-java-sdk-kms" % "1.11.496"
libraryDependencies += "com.amazonaws" % "aws-java-sdk-s3" % "1.11.496"
libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % "2.8.0"
libraryDependencies += "joda-time" % "joda-time" % "2.10.1"
libraryDependencies += "org.apache.httpcomponents" % "httpclient" % "4.5.7"
libraryDependencies ++= Seq(
  "com.typesafe" % "config" % versionTypesafeConfig,
  "com.github.pureconfig" %% "pureconfig" % "0.7.2" exclude (org = "xml-apis", name = "xml-apis"),
  "xml-apis"              % "xml-apis"    % "1.4.01"
)

libraryDependencies += "com.typesafe.play" %% "play-json" % "2.7.2"
