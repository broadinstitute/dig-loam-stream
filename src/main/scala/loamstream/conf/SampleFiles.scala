package loamstream.conf

import java.nio.file.Path

import utils.LoamFileUtils

/**
  * LoamStream
  * Created by oliverr on 3/2/2016.
  */
object SampleFiles {

  object PropertyKeys {
    val hailVds = "hail.vds"
    val hailVcf = "hail.vcf"
    val singletons = "singletons"
    val miniVcf = "sample.file.vcf.mini"
    val samples = "sample.file.samples"
  }

  def getFileFromProperties(key: String): Option[Path] = LProperties.get(key).map(LoamFileUtils.resolveRelativePath)

  val hailVdsOpt: Option[Path] = getFileFromProperties(PropertyKeys.hailVds)
  val hailVcfOpt: Option[Path] = getFileFromProperties(PropertyKeys.hailVcf)
  val singletonsOpt: Option[Path] = getFileFromProperties(PropertyKeys.singletons)
  val miniVcfOpt: Option[Path] = getFileFromProperties(PropertyKeys.miniVcf)
  val samplesOpt: Option[Path] = getFileFromProperties(PropertyKeys.samples)

}
