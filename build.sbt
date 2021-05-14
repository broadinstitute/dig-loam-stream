import sbt.project

lazy val Versions = new {
  val ApacheCommonsIO = "2.6"
  val GoogleCloudStorage = "1.77.0"
  val GoogleAuth = "0.16.1"
  val LogBack = "1.2.3"
  val Scala212 = "2.12.13"
  val Scala213 = "2.13.5"
  lazy val supportedScalaVersions = Seq(Scala212, Scala213)
  val ScalaFmt = "2.7.5"
  val ScalaTest = "3.0.8"
  val Scallop = "3.3.0"
  val TypesafeConfig = "1.3.4"
  val Slick = "3.3.2"
  val RxScala = "0.26.5"
  val Ficus = "1.4.7"
  val Squants = "1.7.4"
  val LogbackColorizer = "1.0.1"
  val Janino = "3.0.12"
  val CommonsCsv = "1.7"
  val DigAws = "0.3.1-SNAPSHOT"
  val HsqlDb = "2.5.0"
  val TestContainersScala = "0.35.2"
  val MysqlConnector = "8.0.19"
  val Sttp = "2.0.6"
  val CommonsCompress = "1.20"
  val Breeze = "1.1"
  val Monix = "3.3.0"
  val ScalaCollectionCompat = "2.2.0"
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
  "org.scalameta" %% "scalafmt-core" % Versions.ScalaFmt,
  "commons-io" % "commons-io" % Versions.ApacheCommonsIO,
  "ch.qos.logback" % "logback-classic" % Versions.LogBack,
  "com.typesafe" % "config" % Versions.TypesafeConfig,
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
  "io.monix" %% "monix" % Versions.Monix,
  "org.scala-lang.modules" %% "scala-collection-compat" % Versions.ScalaCollectionCompat
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
    crossScalaVersions := Versions.supportedScalaVersions,
    scalacOptions ++= Seq("-feature", "-deprecation", "-unchecked"),
    resolvers ++= Seq(Resolvers.SonatypeReleases, Resolvers.SonatypeSnapshots),
    publishTo := Some(Resolvers.LocalRepo),
    libraryDependencies ++= (mainDeps ++ {
      Seq(
        "org.scala-lang" % "scala-library" % scalaVersion.value,
        "org.scala-lang" % "scala-compiler" % scalaVersion.value,
        "org.scala-lang" % "scala-reflect" % scalaVersion.value)
    } ++ testDeps),
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
assembly / test := {}

//Make integration tests run serially; this is needed since some integration tests use Uger, and we can only have
//one Uger/DRMAA session active at once.
//TODO: See if this is still necessary, now that DRMAA is gone.
IntegrationTest / parallelExecution := false

//Show full stack traces from unit and integration tests (F); display test run times (D)
IntegrationTest / testOptions += Tests.Argument("-oFD")
Test / testOptions += Tests.Argument("-oFD")

assembly / assemblyMergeStrategy := {
  {
    case PathList("META-INF", xs @ _*) => MergeStrategy.discard
    case _ => MergeStrategy.first
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
Test / scalastyleConfig := file("scalastyle-config-for-tests.xml")

//Enables `buildInfoTask`, which bakes git version info into the LS jar.
enablePlugins(GitVersioning)

val buildInfoTask = taskKey[Seq[File]]("buildInfo")

buildInfoTask := {
  val dir = (Compile / resourceManaged).value
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

(Compile / resourceGenerators) += buildInfoTask.taskValue
