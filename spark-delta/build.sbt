import sbt.ExclusionRule

name := "spark_delta"

version := "0.1"

scalaVersion := "2.13.7"
val sparkVersion = "3.2.1"

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-streaming" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion

libraryDependencies += "org.apache.spark" %% "spark-hive" % sparkVersion
libraryDependencies += "io.minio" % "minio" % "8.3.7"
libraryDependencies += "io.delta" %% "delta-core" % "1.1.0"

libraryDependencies += "com.amazonaws" % "aws-java-sdk" % "1.11.534"
libraryDependencies += "com.amazonaws" % "aws-java-sdk-bundle" % "1.11.375"
libraryDependencies += "org.postgresql" % "postgresql" % "42.2.16"

libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % "3.3.1" excludeAll(
  ExclusionRule("javax.servlet", "servlet-api"),
  ExclusionRule("javax.servlet.jsp", "jsp-api"),
  ExclusionRule("com.amazonaws", "aws-java-sdk"),
  ExclusionRule("org.mortbay.jetty", "servlet-api")
)

libraryDependencies += "com.typesafe" % "config" % "1.4.1"
libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.9.4"

// https://mvnrepository.com/artifact/org.apache.hive/hive-exec
//libraryDependencies += "org.apache.hive" % "hive-exec" % "3.0.0" excludeAll
//  ExclusionRule(organization = "org.pentaho")


// https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-common
libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "3.3.1" excludeAll(
  ExclusionRule(organization = "javax.servlet"),
  ExclusionRule(organization = "org.spark-project.hive")
)


assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

fork in Test := true