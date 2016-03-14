package loamstream.conf

import java.nio.file.Path

import utils.FileUtils
import scala.util.Try

/**
  * LoamStream
  * Created by oliverr on 3/2/2016.
  */
final case class SampleFiles(props: LProperties) {

  import SampleFiles.PropertyKeys
  
  lazy val miniVcfOpt: Option[Path] = getFileFromProperties(PropertyKeys.miniVcf)
  
  lazy val samplesOpt: Option[Path] = getFileFromProperties(PropertyKeys.samples)
  
  private def getFileFromProperties(key: String): Option[Path] = {
    for {
      path <- props.getString(key)
      resolvedPath <- FileUtils.resolveRelativePath(path)
    } yield resolvedPath
  }
}

object SampleFiles {
  object PropertyKeys {
    val miniVcf = "sampleFiles.vcf.mini"
    val samples = "sampleFiles.samples"
  }
}