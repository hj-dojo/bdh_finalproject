import AssemblyKeys._

assemblySettings

name := "bdhproject_sepsis"

version := "1.0"

scalaVersion := "2.11.8"

resolvers ++= Seq(
  Resolver.sonatypeRepo("releases"),
  Resolver.sonatypeRepo("snapshots")
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.3.0",
  "org.apache.spark" %% "spark-sql" % "2.3.0",
  "org.apache.spark" %% "spark-mllib" % "2.3.0",
  "com.github.fommil.netlib" % "all" % "1.1.2"
)

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.3" % "test"


mainClass in assembly := Some("bdhproject.sepsis.main.Main")

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
