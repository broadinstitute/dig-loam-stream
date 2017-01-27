import sbt.project

lazy val Versions = new {
  val App = "1.2"
  val ApacheCommonsIO = "2.4"
  val DrmaaCommon = "1.0"
  val DrmaaGridEngine = "6.2u5"
  val GoogleCloudStorage = "0.7.0"
  val GoogleAuth = "0.6.0"
  val Htsjdk = "2.1.0"
  val LogBack = "1.1.6"
  val Scala = "2.11.8"
  val Scalariform = "0.1.8"
  val ScalaMajor = "2.11"
  val ScalaTest = "3.0.0"
  val Scallop = "2.0.2"
  val TypesafeConfig = "1.3.0"
  val Slick = "3.1.1"
  val H2 = "1.4.192"
  val RxScala = "0.26.4"
}

lazy val mainDeps = Seq(
  "org.scala-lang" % "scala-library" % Versions.Scala,
  "org.scala-lang" % "scala-compiler" % Versions.Scala,
  "org.scala-lang" % "scala-reflect" % Versions.Scala,
  "org.scalariform" %% "scalariform" % Versions.Scalariform,
  "com.github.samtools" % "htsjdk" % Versions.Htsjdk,
  "commons-io" % "commons-io" % Versions.ApacheCommonsIO,
  "us.levk" % "drmaa-common" % Versions.DrmaaCommon,
  "us.levk" % "drmaa-gridengine" % Versions.DrmaaGridEngine,
  "ch.qos.logback" % "logback-classic" % Versions.LogBack,
  "com.typesafe" % "config" % Versions.TypesafeConfig,
  "io.reactivex" %% "rxscala" % Versions.RxScala,
  "com.typesafe.slick" %% "slick" % Versions.Slick,
  "com.h2database" % "h2" % Versions.H2,
  "org.rogach" %% "scallop" % Versions.Scallop,
  "com.google.cloud" % "google-cloud-storage" % Versions.GoogleCloudStorage,
  "com.google.auth" % "google-auth-library-credentials" % Versions.GoogleAuth
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
    mainClass in assembly := Some("loamstream.apps.Main"),
    mainClass in Compile := Some("loamstream.apps.Main")
  ).enablePlugins(JavaAppPackaging)

lazy val webui = (project in file("webui"))
  .dependsOn(root)
  .enablePlugins(PlayScala)
  .settings(commonSettings: _*)
  .settings(
    name := "LoamStream WebUI"
  )

enablePlugins(GitVersioning)

val buildInfoTask = taskKey[Seq[File]]("buildInfo")

buildInfoTask := {
  val dir = (resourceManaged in Compile).value
  val n = name.value
  val v = version.value
  val branch = git.gitCurrentBranch.value
  val lastCommit = git.gitHeadCommit.value
  val describedVersion = git.gitDescribedVersion.value
  val anyUncommittedChanges = git.gitUncommittedChanges.value

  val buildDate = java.time.Instant.now

  val file = dir / "versionInfo.properties"

  val contents = s"name=${n}\nversion=${v}\nbranch=${branch}\nlastCommit=${lastCommit.getOrElse("")}\nuncommittedChanges=${anyUncommittedChanges}\ndescribedVersion=${describedVersion.getOrElse("")}\nbuildDate=${buildDate}\n"

  IO.write(file, contents)

  Seq(file)
}

(resourceGenerators in Compile) += buildInfoTask.taskValue

/*
 * Command line to run: sbt convertLoams
 *
 * Cross-compiles all the .loam files in src/main/loam/ to .scala files in target/scala-2.11/src_managed/main/ .
 * The output dir is the default value for the SBT setting 'sourceManaged', which is treated specially by SBT and IDEs
 * with SBT support. This makes it easier to load the generated .scala files in an IDE and see red squiggles for any
 * compile errors. In Eclipse, I refresh the project, and target/scala-2.11/src_managed/main/ is automatically picked
 * up as a source of .scala files to be compiled. IntelliJ can likely do the same.
 * 
 * NOTE: This won't run automatically as part of any SBT build steps.  'convertLoams' needs to be
 *       run explicitly.
 */
val convertLoams = taskKey[Unit]("convertLoams")

//TODO: Add this to sourceGenerators somehow
//TODO: Don't hard-code output dir; unfortunately, (sourceManaged in Compile) doesn't work :(
convertLoams := (runMain in Compile).toTask(s" loamstream.util.LoamToScalaConverter src/main/loam/ target/scala-${Versions.ScalaMajor}/src_managed/main/").value


