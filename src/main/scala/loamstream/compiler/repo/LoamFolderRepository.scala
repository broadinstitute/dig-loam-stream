package loamstream.compiler.repo

import java.nio.charset.StandardCharsets
import java.nio.file.{DirectoryStream, Files, Path}

import loamstream.compiler.messages.{LoadResponseMessage, SaveResponseMessage}
import loamstream.util.{Shot, Files => LSFiles}
import loamstream.util.StringUtils

/** A repository of Loam scripts stored in a folder
  *
  * @param folder Folder to contain the Loam scripts
  */
final case class LoamFolderRepository(folder: Path) extends LoamRepository.Mutable {
  override def list: Seq[String] = {
    
    import LoamFolderRepository.Filter
    import scala.collection.JavaConverters._
    
    val entries = Files.newDirectoryStream(folder, Filter).iterator.asScala
    
    def toFileName(path: Path): String = path.getFileName.toString
    
    def removeExtension(name: String): String = name.dropRight(LoamRepository.fileSuffix.length)
    
    entries.map(toFileName).map(removeExtension).toIndexedSeq
  }

  private def nameToPath(name: String): Path = folder.resolve(s"$name${LoamRepository.fileSuffix}")

  override def load(name: String): Shot[LoadResponseMessage] =
    Shot {
      val content = LSFiles.readFromAsUtf8(nameToPath(name))
      
      LoadResponseMessage(name, content, s"Got '$name' from $folder.")
    }

  override def save(name: String, content: String): Shot[SaveResponseMessage] =
    Shot {
      LSFiles.writeTo(nameToPath(name))(content)
      
      SaveResponseMessage(name, s"Added '$name' to $folder.")
    }
}

object LoamFolderRepository {
  private object Filter extends DirectoryStream.Filter[Path] {
    override def accept(entry: Path): Boolean = entry.toString.endsWith(LoamRepository.fileSuffix)
  }
}
