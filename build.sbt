import AssemblyKeys._

assemblySettings

name := "endive"

version := "0.1"


scalaVersion := "2.10.4"

parallelExecution in Test := false

fork := true

{
  val defaultSparkVersion = "1.6.0"
  val sparkVersion = scala.util.Properties.envOrElse("SPARK_VERSION", defaultSparkVersion)
  val defaultHadoopVersion = "2.6.0-cdh5.8.0"
  val hadoopVersion =
    scala.util.Properties.envOrElse("SPARK_HADOOP_VERSION", defaultHadoopVersion)
  val excludeHadoop = ExclusionRule(organization = "org.apache.hadoop")
  val excludeSpark = ExclusionRule(organization = "org.apache.spark")
  val exclude1 = ExclusionRule(organization = "com.google.guava")
  val exclude2 = ExclusionRule(organization = "javax.servlet")
  val exclude3= ExclusionRule(organization = "asm")
  val exclude4= ExclusionRule(organization = "org.jboss.netty")
  val exclude5= ExclusionRule(organization = "org.codehaus.jackson")
  val exclude6= ExclusionRule(organization = "org.sonatype.sisu.inject")
  libraryDependencies ++= Seq(
  "javax.servlet" % "javax.servlet-api" % "3.0.1",
  "edu.arizona.sista" % "processors" % "3.0" exclude("ch.qos.logback", "logback-classic"),
  "edu.arizona.sista" % "processors" % "3.0" classifier "models",
  "org.slf4j" % "slf4j-api" % "1.7.2",
  "org.slf4j" % "slf4j-log4j12" % "1.7.2",
  "org.scalatest" %% "scalatest" % "2.2.6" % "test",
  "org.mockito" % "mockito-core" % "1.8.5",
  "org.apache.commons" % "commons-compress" % "1.7",
  "commons-io" % "commons-io" % "2.4",
  "org.scalanlp" % "breeze_2.10" % "0.11.2",
  "com.github.fommil.netlib" % "all" % "1.1.2" pomOnly(),
  "com.github.scopt" %% "scopt" % "3.3.0",
  "org.apache.parquet" % "parquet-avro" % "1.8.1",
  "org.bdgenomics.utils" %% "utils-misc" % "0.2.7" % "test" classifier "tests",
  "org.bdgenomics.utils" %% "utils-misc" % "0.2.7",
  "org.bdgenomics.utils" %% "utils-cli" % "0.2.7",
  "org.bdgenomics.utils" %% "utils-metrics" % "0.2.7" ,
  "org.bdgenomics.adam" %% "adam-core" % "0.19.2-SNAPSHOT",
  "org.apache.spark" % "spark-core_2.10" % sparkVersion excludeAll(excludeHadoop),
  "org.apache.spark" % "spark-mllib_2.10" % sparkVersion excludeAll(excludeHadoop),
  "org.apache.spark" % "spark-sql_2.10" % sparkVersion excludeAll(excludeHadoop),
  "edu.berkeley.cs.amplab" % "keystoneml_2.10" % "0.3.1-SNAPSHOT" excludeAll(excludeHadoop, excludeSpark),
  "org.yaml" % "snakeyaml" % "1.16",
  "org.apache.commons" % "commons-csv" % "1.2",
    "com.amazonaws" % "aws-java-sdk" % "1.9.40",
    "com.github.seratch" %% "awscala" % "0.5.+",
    "net.jafama" % "jafama" % "2.1.0",
    "com.github.melrief" %% "purecsv" % "0.0.4"
    ,compilerPlugin("org.scalamacros" % "paradise" % "2.0.1" cross CrossVersion.full)
    ,"org.apache.hadoop" % "hadoop-aws" % hadoopVersion
    ,"org.apache.hadoop" % "hadoop-client" % hadoopVersion excludeAll(exclude1, exclude2, exclude3, exclude4, exclude5, exclude6)
)
}

dependencyOverrides ++= Set(
  "com.amazonaws" % "aws-java-sdk" % "1.9.40",
  "com.amazonaws" % "aws-java-sdk-core" % "1.9.40",
  "com.amazonaws" % "aws-java-sdk-s3" % "1.9.40"
)

{
resolvers ++= Seq(
  "Typesafe" at "http://repo.typesafe.com/typesafe/releases",
  "Cloudera Repository" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
  "Spray" at "http://repo.spray.cc",
  "Bintray" at "http://dl.bintray.com/jai-imageio/maven/",
  "ImageJ Public Maven Repo" at "http://maven.imagej.net/content/groups/public/"
  )
}

resolvers += Resolver.sonatypeRepo("public")

resolvers += Resolver.mavenLocal

resolvers += "Local Maven Repository" at "file:///"+Path.userHome+"/.m2/repository"

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
