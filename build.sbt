import AssemblyKeys._

assemblySettings

name := "endive"

version := "0.1"

organization := "edu.berkeley.cs.amplab"

scalaVersion := "2.10.4"

parallelExecution in Test := false

fork := true

javacOptions ++= Seq("-source", "1.7", "-target", "1.7")

{
  val sparkVersion = "1.6.1"
  val excludeHadoop = ExclusionRule(organization = "org.apache.hadoop")
  val excludeSpark = ExclusionRule(organization = "org.apache.spark")
  libraryDependencies ++= Seq(
    "org.apache.spark" % "spark-core_2.10" % sparkVersion excludeAll(excludeHadoop),
    "org.apache.spark" % "spark-mllib_2.10" % sparkVersion excludeAll(excludeHadoop),
    "org.apache.spark" % "spark-sql_2.10" % sparkVersion excludeAll(excludeHadoop),
    "edu.berkeley.cs.amplab" % "keystoneml_2.10" % "0.3.0" excludeAll(excludeHadoop, excludeSpark), 
    "org.yaml" % "snakeyaml" % "1.16",
    "org.apache.commons" % "commons-csv" % "1.2",
    "com.amazonaws" % "aws-java-sdk" % "1.9.40",
    "com.github.seratch" %% "awscala" % "0.5.+",
    "net.jafama" % "jafama" % "2.1.0"
  )
}

libraryDependencies ++= Seq(
  "edu.arizona.sista" % "processors" % "3.0" exclude("ch.qos.logback", "logback-classic"),
  "edu.arizona.sista" % "processors" % "3.0" classifier "models",
  "org.slf4j" % "slf4j-api" % "1.7.2",
  "org.slf4j" % "slf4j-log4j12" % "1.7.2",
  "org.scalatest" %% "scalatest" % "1.9.1" % "test",
  "org.mockito" % "mockito-core" % "1.8.5",
  "org.apache.commons" % "commons-compress" % "1.7",
  "commons-io" % "commons-io" % "2.4",
  "org.scalanlp" % "breeze_2.10" % "0.11.2",
  "com.google.guava" % "guava" % "14.0.1",
  "com.github.fommil.netlib" % "all" % "1.1.2" pomOnly(),
  "com.github.scopt" %% "scopt" % "3.3.0",
  "org.apache.parquet" % "parquet-avro" % "1.8.1",
  "org.bdgenomics.utils" %% "utils-misc" % "0.2.7",
  "org.bdgenomics.utils" %% "utils-misc" % "0.2.7" % "test",
  "org.bdgenomics.utils" %% "utils-cli" % "0.2.7",
  "org.bdgenomics.utils" %% "utils-metrics" % "0.2.7",
  "org.bdgenomics.adam" %% "adam-core" % "0.19.1-SNAPSHOT"
)

libraryDependencies ++= Seq(
    "com.github.melrief" %% "purecsv" % "0.0.4"
  , compilerPlugin("org.scalamacros" % "paradise" % "2.0.1" cross CrossVersion.full)
)

dependencyOverrides ++= Set(
  "com.amazonaws" % "aws-java-sdk" % "1.9.40",
  "com.amazonaws" % "aws-java-sdk-core" % "1.9.40",
  "com.amazonaws" % "aws-java-sdk-s3" % "1.9.40"
)

{
  val defaultHadoopVersion = "2.6.0"
  val hadoopVersion =
    scala.util.Properties.envOrElse("SPARK_HADOOP_VERSION", defaultHadoopVersion)
  libraryDependencies ++= Seq("org.apache.hadoop" % "hadoop-aws" % hadoopVersion,
    "org.apache.hadoop" % "hadoop-client" % hadoopVersion)
}

resolvers ++= Seq(
  "Typesafe" at "http://repo.typesafe.com/typesafe/releases",
  "Cloudera Repository" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
  "Spray" at "http://repo.spray.cc",
  "Bintray" at "http://dl.bintray.com/jai-imageio/maven/",
  "ImageJ Public Maven Repo" at "http://maven.imagej.net/content/groups/public/"
)

resolvers += Resolver.sonatypeRepo("public")

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
  {
    case PathList("javax", "servlet", xs @ _*)               => MergeStrategy.first
    case PathList(ps @ _*) if ps.last endsWith ".html"       => MergeStrategy.first
    case "application.conf"                                  => MergeStrategy.concat
    case "reference.conf"                                    => MergeStrategy.concat
    case "log4j.properties"                                  => MergeStrategy.first
    case m if m.toLowerCase.endsWith("manifest.mf")          => MergeStrategy.discard
    case m if m.toLowerCase.matches("meta-inf.*\\.sf$")      => MergeStrategy.discard
    case m if m.toLowerCase.startsWith("meta-inf/services/") => MergeStrategy.filterDistinctLines
      // This line required for SCIFIO stuff to work correctly, due to some dependency injection stuff
    case "META-INF/json/org.scijava.plugin.Plugin" => MergeStrategy.concat
    case _ => MergeStrategy.first
  }
}

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false, includeDependency = false)

test in assembly := {}
