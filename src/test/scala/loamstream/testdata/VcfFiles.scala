package loamstream.testdata

import java.io.File
import java.nio.file.{Path, Paths}

import utils.FileUtils

/**
  * LoamStream
  * Created by oliverr on 3/2/2016.
  */
object VcfFiles {

  val miniPathLocal = "mini.vcf"
  val miniPath = FileUtils.resolveRelativePath(miniPathLocal)

}
