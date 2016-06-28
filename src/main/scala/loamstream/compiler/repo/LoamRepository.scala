package loamstream.compiler.repo

import java.nio.file.Path

import loamstream.compiler.messages.{LoadResponseMessage, SaveResponseMessage}
import loamstream.util.Shot

/**
  * LoamStream
  * Created by oliverr on 6/1/2016.
  */
object LoamRepository {
  val defaultPackageName = "loam"
  val fileSuffix = ".loam"
  val defaultEntries = Seq("first", "impute")
  val defaultRepo: LoamRepository.Mutable = inMemory ++ ofPackage(defaultPackageName, defaultEntries)

  def ofFolder(path: Path): LoamFolderRepository = LoamFolderRepository(path)

  def ofPackage(packageName: String, entries: Seq[String]): LoamPackageRepository =
    LoamPackageRepository(packageName, entries)

  def ofMap(entries: Map[String, String]): LoamMapRepository = LoamMapRepository(entries)

  def inMemory: LoamMapRepository = ofMap(Map.empty)

  trait Mutable extends LoamRepository {
    def save(name: String, content: String): Shot[SaveResponseMessage]

    def ++(that: LoamRepository): LoamComboRepository = LoamComboRepository(this, that)
  }

}

trait LoamRepository {
  def list: Seq[String]

  def load(name: String): Shot[LoadResponseMessage]
}
