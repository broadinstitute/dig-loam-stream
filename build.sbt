import sbt.project

lazy val Versions = new {
  val ApacheCommonsIO = "2.6"
  val GoogleCloudStorage = "1.77.0"
  val GoogleAuth = "0.16.1"
  val LogBack = "1.2.3"
  val Scala = "2.12.12"
  val ScalaMajor = "2.12"
  val ScalaFmt = "1.5.1"
  val ScalaTest = "3.0.8"
  val Scallop = "3.3.0"
  val TypesafeConfig = "1.3.4"
  val Slick = "3.3.2"
  val RxScala = "0.26.5"
  val Ficus = "1.4.7"
  val Squants = "1.4.0"
  val LogbackColorizer = "1.0.1"
  val Janino = "3.0.12"
  val CommonsCsv = "1.7"
  val DigAws = "0.1-SNAPSHOT"
  val HsqlDb = "2.5.0"
  val TestContainersScala = "0.35.2"
  val MysqlConnector = "8.0.19"
  val Sttp = "2.0.6"
  val CommonsCompress = "1.20"
  val Breeze = "1.1"
  val RequestsScala = "0.6.7"
}

lazy val Orgs = new {
  val DIG = "org.broadinstitute.dig"
}

lazy val Paths = new {
  //`publish` will produce artifacts under this path
  val LocalRepo = "/humgen/diabetes/users/dig/loamstream/repo"
}

lazy val Resolvers = new {
  val LocalRepo = Resolver.file("localRepo", new File(Paths.LocalRepo))
  val SonatypeReleases = Resolver.sonatypeRepo("releases")
  val SonatypeSnapshots = Resolver.sonatypeRepo("snapshots")
}

lazy val mainDeps = Seq(
  "org.scala-lang" % "scala-library" % Versions.Scala,
  "org.scala-lang" % "scala-compiler" % Versions.Scala,
  "org.scala-lang" % "scala-reflect" % Versions.Scala,
  "com.geirsson" %% "scalafmt-core" % Versions.ScalaFmt,
  "commons-io" % "commons-io" % Versions.ApacheCommonsIO,
  "ch.qos.logback" % "logback-classic" % Versions.LogBack,
  "com.typesafe" % "config" % Versions.TypesafeConfig,
  "io.reactivex" %% "rxscala" % Versions.RxScala,
  "com.typesafe.slick" %% "slick" % Versions.Slick,
  "org.rogach" %% "scallop" % Versions.Scallop,
  "com.google.cloud" % "google-cloud-storage" % Versions.GoogleCloudStorage,
  "com.google.auth" % "google-auth-library-credentials" % Versions.GoogleAuth,
  "com.iheart" %% "ficus" % Versions.Ficus,
  "org.typelevel"  %% "squants"  % Versions.Squants,
  "org.tuxdude.logback.extensions" % "logback-colorizer" % Versions.LogbackColorizer,
  "org.codehaus.janino" % "janino" % Versions.Janino,
  "org.apache.commons" % "commons-csv" % Versions.CommonsCsv,
  Orgs.DIG %% "dig-aws" % Versions.DigAws,
  "org.hsqldb" % "hsqldb" % Versions.HsqlDb,
  "com.softwaremill.sttp.client" %% "core" % Versions.Sttp,
  "org.apache.commons" % "commons-compress" % Versions.CommonsCompress,
  "org.scalanlp" %% "breeze" % Versions.Breeze,
  "com.lihaoyi" %% "requests" % Versions.RequestsScala
)

lazy val testDeps = Seq(
  "org.scalatest" %% "scalatest" % Versions.ScalaTest % "it,test",
  "com.dimafeng" %% "testcontainers-scala-scalatest" % Versions.TestContainersScala % "it,test",
  "com.dimafeng" %% "testcontainers-scala-mysql" % Versions.TestContainersScala % "it,test",
  "mysql" % "mysql-connector-java" % Versions.MysqlConnector % "it,test",
)

lazy val root = (project in file("."))
  .configs(IntegrationTest)
  .settings(Defaults.itSettings : _*)
  .settings(
    name := "loamstream",
    organization := Orgs.DIG,
    //NB: version set in version.sbt
    scalaVersion := Versions.Scala,
    scalacOptions ++= Seq("-feature", "-deprecation", "-unchecked"),
    resolvers ++= Seq(Resolvers.SonatypeReleases, Resolvers.SonatypeSnapshots),
    publishTo := Some(Resolvers.LocalRepo),
    libraryDependencies ++= (mainDeps ++ testDeps),
    scalastyleFailOnError := true,
    packageSummary in Linux := "LoamStream - Language for Omics Analysis Management",
    packageSummary in Windows := "LoamStream - Language for Omics Analysis Management",
    packageDescription := "A high level-language and runtime environment for large-scale omics analysis.",
    maintainer in Windows := "Oliver Ruebenacker, Broad Institute, oliverr@broadinstitute.org",
    maintainer in Debian := "Oliver Ruebenacker, Broad Institute, oliverr@broadinstitute.org",
    mainClass in assembly := Some("loamstream.apps.Main"),
    mainClass in Compile := Some("loamstream.apps.Main")
  ).enablePlugins(JavaAppPackaging)

//Skip tests when running assembly (and publishing).  Comment this line to re-enable tests when publishing.
test in assembly := {}

//Make integration tests run serially; this is needed since some integration tests use Uger, and we can only have
//one Uger/DRMAA session active at once.
//TODO: See if this is still necessary, now that DRMAA is gone.
parallelExecution in IntegrationTest := false

//Show full stack traces from unit and integration tests (F); display test run times (D)
testOptions in IntegrationTest += Tests.Argument("-oFD")
testOptions in Test += Tests.Argument("-oFD")

assemblyMergeStrategy in assembly := {
  def hasTakeFirstExtension(s: String): Boolean = {
    s.endsWith(".json") || s.endsWith(".config") || s.endsWith(".properties")
  }

  {
    case PathList(ps @ _*) if hasTakeFirstExtension(ps.last) => MergeStrategy.first
    case x => {
      val oldStrategy = (assemblyMergeStrategy in assembly).value
      oldStrategy(x)
    }
  }
}

//Make the fat jar produced by `assembly` a tracked artifact that will be published.  
//(Without this bit, only the application classes, sources, and docs will be published.)
artifact in (Compile, assembly) := {
  val art = (artifact in (Compile, assembly)).value
  art.withClassifier(Some("assembly"))
}

addArtifact(artifact in (Compile, assembly), assembly)

//Use slightly different style rules for tests.
scalastyleConfig in Test := file("scalastyle-config-for-tests.xml")

//Enables `buildInfoTask`, which bakes git version info into the LS jar.
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
 * Cross-compiles all the .loam files in pipeline/loam/ to .scala files in target/scala-2.12/src_managed/main/ .
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
convertLoams := (runMain in Compile).toTask(s" loamstream.util.LoamToScalaConverter pipeline/loam/ target/scala-${Versions.ScalaMajor}/src_managed/main/").value


