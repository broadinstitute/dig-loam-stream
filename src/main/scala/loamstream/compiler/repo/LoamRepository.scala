package loamstream.compiler.repo

import java.nio.file.Path

import loamstream.compiler.messages.{LoadResponseMessage, SaveResponseMessage}
import loamstream.util.{Hit, Shot}

/**
  * LoamStream
  * Created by oliverr on 6/1/2016.
  */
object LoamRepository {
  val projectName = "dig-loam-stream"
  val resourceFolder = "src/main/resources"
  val defaultPackageName = "loam"
  val defaultEntries = Seq("first", "impute")
  val defaultPackageRepo = ofPackage(defaultPackageName, defaultEntries)
  val fileSuffix = ".loam"

  val defaultRepo: LoamRepository.Mutable = {
    defaultPackageRepo.shootForClassFolder match {
      case Hit(classFolder) =>
        val classFolderDepth =
          classFolder.getNameCount - 1 -
            (0 until classFolder.getNameCount).map(classFolder.getName).map(_.toString).indexOf(projectName)
        val rootFolder = {
          var folder = classFolder
          for (i <- 0 until classFolderDepth) {
            folder = folder.getParent
          }
          folder
        }
        val loamFolder = rootFolder.resolve(s"$resourceFolder/$defaultPackageName")
        ofFolder(loamFolder)
      case _ => inMemory ++ defaultPackageRepo
    }
  }

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
