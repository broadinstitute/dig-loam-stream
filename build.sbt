import sbt.project

lazy val Versions = new {
  val App = "0.1"
  val Scala = "2.11.7"
  val ScalaTest = "2.2.6"
  val Htsjdk = "2.1.0"
}

lazy val mainDeps = Seq(
  "org.scala-lang" % "scala-library" % Versions.Scala,
  "org.scala-lang" % "scala-compiler" % Versions.Scala,
  "org.scala-lang" % "scala-reflect" % Versions.Scala,
  "com.github.samtools" % "htsjdk" % Versions.Htsjdk
)

lazy val testDeps = Seq(
  "org.scalatest" %% "scalatest" % Versions.ScalaTest % Test
)

lazy val commonSettings = Seq(
  version := Versions.App,
  scalaVersion := Versions.Scala,
  scalacOptions ++= Seq("-feature"),
  resolvers ++= Seq(
    Resolver.sonatypeRepo("releases"),
    Resolver.sonatypeRepo("snapshots")
  ),
  libraryDependencies ++= (mainDeps ++ testDeps),
  scalastyleFailOnError := true
)

lazy val root = (project in file("."))
  .settings(commonSettings: _*)
  .settings(
    name := "LoamStream",
    packageSummary in Linux := "LoamStream - Language for Omics Analysis Management",
    packageSummary in Windows := "LoamStream - Language for Omics Analysis Management",
    packageDescription := "A high level-language and runtime environment for large-scale omics analysis.",
    maintainer in Windows := "Oliver Ruebenacker, Broad Institute, oliverr@broadinstitute.org",
    maintainer in Debian := "Oliver Ruebenacker, Broad Institute, oliverr@broadinstitute.org",
    mainClass in Compile := Some("loamstream.apps.LapRunApp")
  ).enablePlugins(JavaAppPackaging)
