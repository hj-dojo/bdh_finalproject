import AssemblyKeys._

assemblySettings

name := "big-data-project"

version := "1.0"

scalaVersion := "2.10.6"

resolvers ++= Seq(
  Resolver.sonatypeRepo("releases"),
  Resolver.sonatypeRepo("snapshots")
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.6.1",
  "org.apache.spark" %% "spark-sql" % "1.6.1",
  "org.apache.spark" %% "spark-mllib" % "1.6.1",
  "com.databricks" %% "spark-csv" % "1.4.0",
  "com.github.fommil.netlib" % "all" % "1.1.2"
)

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.6" % "test"


mainClass in assembly := Some("edu.gatech.cse8803.main.Main")

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
{
  case m if m.toLowerCase.endsWith("manifest.mf") => MergeStrategy.discard
  case m if m.startsWith("META-INF") => MergeStrategy.discard
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.first
  case PathList("org", "apache", xs @ _*) => MergeStrategy.first
  case PathList("org", "jboss", xs @ _*) => MergeStrategy.first
  case "about.html"  => MergeStrategy.rename
  case "reference.conf" => MergeStrategy.concat
  case _ => MergeStrategy.first
}
}

parallelExecution in Test := false
