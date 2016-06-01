package loamstream.compiler

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}

import loamstream.util.Shot

import scala.util.Try

/**
  * LoamStream
  * Created by oliverr on 6/1/2016.
  */
object LoamRepository {
  val defaultFolderName = "loam"
  val default = LoamRepository(Paths.get(defaultFolderName))
}

case class LoamRepository(folder: Path) {
  def get(fileName: String): Shot[String] =
    Shot.fromTry(Try {
      val filePath = folder.resolve(fileName)
      new String(Files.readAllBytes(filePath), StandardCharsets.UTF_8)
    })
}
