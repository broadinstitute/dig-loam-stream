package loamstream.compiler.repo

import java.nio.charset.StandardCharsets
import java.nio.file.{DirectoryStream, Files, Path}

import loamstream.compiler.messages.{LoadResponseMessage, SaveResponseMessage}
import loamstream.util.{Shot, Files => LSFiles}

import scala.util.Try

/** A repository of Loam scripts stored in a folder
  *
  * @param folder Folder to contain the Loam scripts
  */
case class LoamFolderRepository(folder: Path) extends LoamRepository.Mutable {
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
      .map(name => name.substring(0, name.length - LoamRepository.fileSuffix.length))
  }

  def nameToPath(name: String): Path = folder.resolve(s"$name${LoamRepository.fileSuffix}")

  override def load(name: String): Shot[LoadResponseMessage] =
    Shot.fromTry(Try {
      val content = new String(Files.readAllBytes(nameToPath(name)), StandardCharsets.UTF_8)
      LoadResponseMessage(name, content, s"Got '$name' from $folder.")
    })

  override def save(name: String, content: String): Shot[SaveResponseMessage] =
    Shot.fromTry(Try {
      LSFiles.writeTo(nameToPath(name))(content)
      SaveResponseMessage(name, s"Added '$name' to $folder.")
    })

}
