lazy val root = (project in file(".")).
  settings(
    name := "LoamStream",
    version := "0.1",
    scalaVersion := "2.11.7",
    resolvers ++= Seq(
      Resolver.sonatypeRepo("releases"),
      Resolver.sonatypeRepo("snapshots")
    ),
    libraryDependencies ++= Seq(
      "com.chuusai" %% "shapeless" % "2.2.5",
      "org.scala-lang" % "scala-library" % scalaVersion.value,
      "org.scala-lang" % "scala-compiler" % scalaVersion.value,
      "org.scala-lang" % "scala-reflect" % scalaVersion.value)
  )
