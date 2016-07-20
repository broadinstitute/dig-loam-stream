package loamstream

import loamstream.conf.{LProperties}
import loamstream.conf.SampleFiles

object TestData {
  lazy val props = LProperties.load("loamstream-test")

  lazy val sampleFiles = SampleFiles(props)
}