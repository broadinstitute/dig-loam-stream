package loamstream.testdata

import java.io.File
import java.nio.file.{Path, Paths}

/**
  * LoamStream
  * Created by oliverr on 3/2/2016.
  */
object VcfFiles {

  def toPath(localPath: String): Path = new File(getClass.getResource(localPath).toURI).toPath

  val miniPathLocal = "mini.vcf"
  val miniPath = toPath(miniPathLocal)

}
