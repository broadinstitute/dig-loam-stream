resolvers += "Typesafe repository" at "https://dl.bintray.com/typesafe/maven-releases/"

resolvers += Resolver.url("hmrc-sbt-plugin-releases (sbt-git-stamp)", url("https://dl.bintray.com/hmrc/sbt-plugin-releases"))(Resolver.ivyStylePatterns)

addSbtPlugin("uk.gov.hmrc" % "sbt-git-stamp" % "5.3.0")

addSbtPlugin("com.typesafe.sbt" % "sbt-native-packager" % "1.1.1")

addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "0.8.0")

addSbtPlugin("com.typesafe.play" % "sbt-plugin" % "2.5.3")

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.3")

