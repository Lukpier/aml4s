name := "aml4s"

version := "0.1"

scalaVersion := "2.11.8"
scalafmtOnCompile := true

val sparkVersion = "2.2.0"
val sparkTestingVersion = "0.14.0"

resolvers ++= Seq(
  "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
  "mvnrepository" at "https://mvnrepository.com/artifact/",
  "cloudera repo" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
  "spark pack" at "https://dl.bintray.com/spark-packages/maven/"
)

libraryDependencies ++= Seq(
  "org.scalanlp" %% "breeze" % "1.0",
  "org.apache.commons" % "commons-math3" % "3.6.1",
  "org.apache.spark" %% s"spark-core" % s"${sparkVersion}" % Provided,
  "org.apache.spark" %% s"spark-sql" % s"${sparkVersion}" % Provided,
  "org.apache.spark" %% s"spark-mllib" % s"${sparkVersion}" % Provided,
  "org.apache.spark" %% s"spark-hive" % s"${sparkVersion}" % Provided,
  "com.holdenkarau" %% "spark-testing-base" % s"${sparkVersion}_${sparkTestingVersion}" % Test,
  "org.scalatest" %% "scalatest" % "3.2.9" % Test,
  "org.xerial.snappy" % "snappy-java" % "1.1.8.4"
)
