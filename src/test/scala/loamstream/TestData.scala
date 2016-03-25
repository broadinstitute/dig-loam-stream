package loamstream

import loamstream.conf.{LProperties, SampleFiles}

object TestData {
  lazy val props = LProperties.load("loamstream-test")

  lazy val sampleFiles = SampleFiles(props)
}