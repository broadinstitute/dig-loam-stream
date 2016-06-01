package loamstream.compiler

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}

import loamstream.util.Shot

import scala.util.Try

/**
  * LoamStream
  * Created by oliverr on 6/1/2016.
  */
object LoamRepository {
  val folderName = "loam"

  def inDefaultDirectoryInRoot(root: Path) = LoamRepository(root.resolve(folderName))
}

case class LoamRepository(folder: Path) {
  def get(fileName: String): Shot[String] =
    Shot.fromTry(Try {
      val filePath = folder.resolve(fileName)
      new String(Files.readAllBytes(filePath), StandardCharsets.UTF_8)
    })
}
