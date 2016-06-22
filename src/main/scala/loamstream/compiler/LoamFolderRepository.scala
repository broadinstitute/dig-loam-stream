package loamstream.compiler

import java.nio.charset.StandardCharsets
import java.nio.file.{DirectoryStream, Files, Path}

import loamstream.util.Shot

import scala.util.Try

/**
  * LoamStream
  * Created by oliverr on 6/3/2016.
  */
case class LoamFolderRepository(folder: Path) extends LoamRepository {
  override def list: Seq[String] = {
    val filter = new DirectoryStream.Filter[Path] {
      override def accept(entry: Path): Boolean = entry.toString.endsWith(".loam")
    }
    val streamIter = Files.newDirectoryStream(folder, filter).iterator()
    var entries: Seq[Path] = Seq.empty
    while (streamIter.hasNext) {
      entries :+= streamIter.next()
    }
    entries.map(path => path.getName(path.getNameCount - 1)).map(_.toString)
      .map(name => name.substring(name.length - 5))
  }

  override def get(name: String): Shot[String] =
    Shot.fromTry(Try {
      val filePath = folder.resolve(name)
      new String(Files.readAllBytes(filePath), StandardCharsets.UTF_8)
    })

}
