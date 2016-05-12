import java.io.File

import sbt.project

lazy val Versions = new {
  val App = "0.1"
  val ApacheCommonsIO = "2.4"
  val Htsjdk = "2.1.0"
  val LogBack = "1.1.6"
  val Scala = "2.11.8"
  val ScalaTest = "2.2.6"
  val TypesafeConfig = "1.3.0"
}

lazy val mainDeps = Seq(
  "org.scala-lang" % "scala-library" % Versions.Scala,
  "org.scala-lang" % "scala-compiler" % Versions.Scala,
  "org.scala-lang" % "scala-reflect" % Versions.Scala,
  "com.github.samtools" % "htsjdk" % Versions.Htsjdk,
  "commons-io" % "commons-io" % Versions.ApacheCommonsIO,
  "ch.qos.logback" % "logback-classic" % Versions.LogBack,
  "com.typesafe" % "config" % Versions.TypesafeConfig
)

lazy val testDeps = Seq(
  "org.scalatest" %% "scalatest" % Versions.ScalaTest % Test
)

lazy val commonSettings = Seq(
  version := Versions.App,
  scalaVersion := Versions.Scala,
  scalacOptions ++= Seq("-feature", "-deprecation", "-unchecked"),
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
    maintainer in Debian := "Oliver Ruebenacker, Broad Institute, oliverr@broadinstitute.org"
  ).enablePlugins(JavaAppPackaging)

val captureSbtClasspath = taskKey[Unit]("sbt-classpath")

lazy val webui = (project in file("webui"))
  .dependsOn(root)
  .enablePlugins(PlayScala)
  .settings(commonSettings: _*)
  .settings(
    captureSbtClasspath := {
      val files: Seq[File] = (fullClasspath in Compile).value.files
      val sbtClasspath: String = files.map(x => x.getAbsolutePath).mkString(File.pathSeparator)
      println("Set SBT classpath to 'sbt-classpath' environment variable") // scalastyle:ignore
      System.setProperty("sbt-classpath", sbtClasspath)
    },
    name := "LoamStream WebUI"
  )
