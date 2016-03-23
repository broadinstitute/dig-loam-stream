package loamstream.conf

import java.nio.file.Path

import utils.LoamFileUtils

/**
  * LoamStream
  * Created by oliverr on 3/2/2016.
  */
final case class SampleFiles(props: LProperties) {

  import SampleFiles.PropertyKeys

  lazy val miniVcfOpt: Option[Path] = getFileFromProperties(PropertyKeys.miniVcf)
  lazy val miniForPcaVcfOpt: Option[Path] = getFileFromProperties(PropertyKeys.miniForPcaVcf)
  lazy val hailVcfOpt: Option[Path] = getFileFromProperties(PropertyKeys.hailVcf)
  lazy val hailVdsOpt: Option[Path] = getFileFromProperties(PropertyKeys.hailVds)
  lazy val singletonsOpt: Option[Path] = getFileFromProperties(PropertyKeys.singletons)

  private def getFileFromProperties(key: String): Option[Path] = {
    for {
      path <- props.getString(key)
      resolvedPath <- LoamFileUtils.resolveRelativePath(path)
    } yield resolvedPath
  }
}

object SampleFiles {

  object PropertyKeys {
    val miniVcf = "sampleFiles.vcf.mini"
    val miniForPcaVcf = "sampleFiles.vcf.miniForPca"
    val hailVcf = "hail.vcf"
    val hailVds = "hail.vds"
    val singletons = "hail.singletons"
  }

}
