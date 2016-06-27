package loamstream.compiler.repo

import java.nio.charset.StandardCharsets
import java.nio.file.{DirectoryStream, Files, Path}

import loamstream.util.Shot
import loamstream.util.{Files => LSFiles}

import scala.util.Try

/**
  * LoamStream
  * Created by oliverr on 6/3/2016.
  */
case class LoamFolderRepository(folder: Path) extends LoamRepository {
  override def list: Seq[String] = {
    val filter = new DirectoryStream.Filter[Path] {
      override def accept(entry: Path): Boolean = entry.toString.endsWith(LoamRepository.fileSuffix)
    }
    val streamIter = Files.newDirectoryStream(folder, filter).iterator()
    var entries: Seq[Path] = Seq.empty
    while (streamIter.hasNext) {
      entries :+= streamIter.next()
    }
    entries.map(path => path.getName(path.getNameCount - 1)).map(_.toString)
      .map(name => name.substring(name.length - LoamRepository.fileSuffix.length))
  }

  def nameToPath(name: String): Path = folder.resolve(s"$name${LoamRepository.fileSuffix}")

  override def get(name: String): Shot[String] =
    Shot.fromTry(Try {
      new String(Files.readAllBytes(nameToPath(name)), StandardCharsets.UTF_8)
    })

  override def add(name: String, content: String): Shot[String] =
    Shot.fromTry(Try {
      LSFiles.writeTo(nameToPath(name))(content)
      name
    })

}

object LoamFolderRepository {
}
