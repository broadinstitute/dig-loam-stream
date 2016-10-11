package loamstream.compiler.repo

import java.nio.file.{DirectoryStream, Files, Path}

import loamstream.loam.LoamScript
import loamstream.util.{Shot, Files => LSFiles}

/** A repository of Loam scripts stored in a folder
  *
  * @param folder Folder to contain the Loam scripts
  */
final case class LoamFolderRepository(folder: Path) extends LoamRepository.Mutable {
  /** Human-readable description on where the scripts are stored */
  override def description: String = s"in folder $folder"

  override def list: Seq[String] = {

    import LoamFolderRepository.Filter

    import scala.collection.JavaConverters._

    val entries = Files.newDirectoryStream(folder, Filter).iterator.asScala

    def toFileName(path: Path): String = path.getFileName.toString

    def removeExtension(name: String): String = name.dropRight(LoamRepository.fileSuffix.length)

    entries.map(toFileName).map(removeExtension).toIndexedSeq
  }

  private def nameToPath(name: String): Path = folder.resolve(s"$name${LoamRepository.fileSuffix}")

  override def save(script: LoamScript): Shot[LoamScript] =
    Shot {
      LSFiles.writeTo(nameToPath(script.name))(script.code)
      script
    }

  /** Loads Loam script of given name from repository  */
  override def load(name: String): Shot[LoamScript] =
  Shot {
    val content = LSFiles.readFromAsUtf8(nameToPath(name))

    LoamScript(name, content)
  }


}

object LoamFolderRepository {

  private object Filter extends DirectoryStream.Filter[Path] {
    override def accept(entry: Path): Boolean = entry.toString.endsWith(LoamRepository.fileSuffix)
  }

}
