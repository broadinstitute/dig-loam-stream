import sbt.project

lazy val Versions = new {
  val App = "0.1"
  val ApacheCommonsIO = "2.4"
  val DrmaaCommon = "1.0"
  val DrmaaGridEngine = "6.2u5"
  val Htsjdk = "2.1.0"
  val LogBack = "1.1.6"
  val Scala = "2.11.8"
  val ScalaTest = "2.2.6"
  val TypesafeConfig = "1.3.0"
  val Monix = "2.0-RC6"
  val ScalaRx = "0.3.1"
}

lazy val mainDeps = Seq(
  "org.scala-lang" % "scala-library" % Versions.Scala,
  "org.scala-lang" % "scala-compiler" % Versions.Scala,
  "org.scala-lang" % "scala-reflect" % Versions.Scala,
  "com.github.samtools" % "htsjdk" % Versions.Htsjdk,
  "commons-io" % "commons-io" % Versions.ApacheCommonsIO,
  "us.levk" % "drmaa-common" % Versions.DrmaaCommon,
  "us.levk" % "drmaa-gridengine" % Versions.DrmaaGridEngine,
  "ch.qos.logback" % "logback-classic" % Versions.LogBack,
  "com.typesafe" % "config" % Versions.TypesafeConfig,
  "io.monix" %% "monix" % Versions.Monix,
  "com.lihaoyi" %% "scalarx" % Versions.ScalaRx
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
    maintainer in Debian := "Oliver Ruebenacker, Broad Institute, oliverr@broadinstitute.org",
    mainClass in assembly := Some("loamstream.apps.LoamRunApp"),
    mainClass in Compile := Some("loamstream.apps.LoamRunApp")
  ).enablePlugins(JavaAppPackaging)

lazy val webui = (project in file("webui"))
  .dependsOn(root)
  .enablePlugins(PlayScala)
  .settings(commonSettings: _*)
  .settings(
    name := "LoamStream WebUI"
  )