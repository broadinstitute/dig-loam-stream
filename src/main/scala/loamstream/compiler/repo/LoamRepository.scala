package loamstream.compiler.repo

import java.nio.file.Path

import loamstream.compiler.messages.{LoadResponseMessage, SaveResponseMessage}
import loamstream.util.{Hit, Shot}

/** A repository of Loam scripts */
trait LoamRepository {
  /** Lists names of all available Loam scripts */
  def list: Seq[String]

  /** Loads Loam script of given name from repository  */
  def load(name: String): Shot[LoadResponseMessage]
}

/** A repository of Loam scripts */
object LoamRepository {
  //TODO: Is this supposed to match what's defined in build.sbt?
  val projectName = "dig-loam-stream"
  val resourceFolder = "src/main/resources"
  val defaultPackageName = "loam"
  val defaultEntries = Seq("first", "toyImpute", "impute", "imputeParallel")
  val defaultPackageRepo = ofPackage(defaultPackageName, defaultEntries)
  val fileSuffix = ".loam"

  /** The default repository
    *
    * Tries to locate resource folder containing the Loam scripts, if that fails, use a combination of in-memory
    * repository and package-based repository.
    */
  val defaultRepo: LoamRepository.Mutable = {
    defaultPackageRepo.shootForClassFolder match {
      case Hit(classFolder) =>
        val classFolderDepth =
          classFolder.getNameCount - 1 -
            (0 until classFolder.getNameCount).map(classFolder.getName).map(_.toString).indexOf(projectName)
        val rootFolder = {
          (0 until classFolderDepth).foldLeft(classFolder) { (folder, _) => folder.getParent }
        }
        val loamFolder = rootFolder.resolve(s"$resourceFolder/$defaultPackageName")
        
        ofFolder(loamFolder)
        
      case _ => inMemory ++ defaultPackageRepo
    }
  }

  /** Creates repository based on folder */
  def ofFolder(path: Path): LoamFolderRepository = LoamFolderRepository(path)

  /** Creates repository based on package */
  def ofPackage(packageName: String, entries: Seq[String]): LoamPackageRepository =
    LoamPackageRepository(packageName, entries)

  /** Creates in-memory repository based on Map */
  def ofMap(entries: Map[String, String]): LoamMapRepository = LoamMapRepository(entries)

  /** Creates in-memory repository based on Map initially empty */
  def inMemory: LoamMapRepository = ofMap(Map.empty)

  /** A repository to which Loam scripts can be saved */
  trait Mutable extends LoamRepository {
    /** Saves Loam script to this repository */
    def save(name: String, content: String): Shot[SaveResponseMessage]

    /** Creates combo repository combining thsi and that repository */
    def ++(that: LoamRepository): LoamComboRepository = LoamComboRepository(this, that)
  }

}
