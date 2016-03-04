package loamstream.conf

import java.nio.file.Path

import utils.FileUtils

/**
  * LoamStream
  * Created by oliverr on 3/2/2016.
  */
object SampleFiles {

  object PropertyKeys {
    val miniVcf = "sample.file.vcf.mini"
    val samples = "sample.file.samples"
  }

  def getFileFromProperties(key: String): Option[Path] = LProperties.get(key).map(FileUtils.resolveRelativePath)

  val miniVcfOpt: Option[Path] = getFileFromProperties(PropertyKeys.miniVcf)
  val samplesOpt: Option[Path] = getFileFromProperties(PropertyKeys.samples)

}
