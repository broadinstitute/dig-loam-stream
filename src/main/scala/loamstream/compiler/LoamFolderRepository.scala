package loamstream.compiler

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}

import loamstream.util.Shot

import scala.util.Try

/**
  * LoamStream
  * Created by oliverr on 6/3/2016.
  */
case class LoamFolderRepository(folder: Path) extends LoamRepository {
  override def get(name: String): Shot[String] =
    Shot.fromTry(Try {
      val filePath = folder.resolve(name)
      new String(Files.readAllBytes(filePath), StandardCharsets.UTF_8)
    })
}
