lazy val root = (project in file(".")).
  settings(
    name := "LoamStream",
    version := "0.1",
    scalaVersion := "2.11.7",
    scalacOptions ++= Seq("-feature"),
    resolvers ++= Seq(
      Resolver.sonatypeRepo("releases"),
      Resolver.sonatypeRepo("snapshots")
    ),
    libraryDependencies ++= Seq(
      "org.scala-lang" % "scala-library" % scalaVersion.value,
      "org.scala-lang" % "scala-compiler" % scalaVersion.value,
      "org.scala-lang" % "scala-reflect" % scalaVersion.value),
    packageSummary in Linux := "LoamStream - Language for Omics Analysis Management",
    packageSummary in Windows := "LoamStream - Language for Omics Analysis Management",
    packageDescription := "A high level-language and runtime environment for large-scale omics analysis.",
    maintainer in Windows := "Oliver Ruebenacker, Broad Institute, oliverr@broadinstitute.org",
    maintainer in Debian := "Oliver Ruebenacker, Broad Institute, oliverr@broadinstitute.org",
    mainClass in Compile := Some("loamstream.apps.LapRunApp")
  ).enablePlugins(JavaAppPackaging)
