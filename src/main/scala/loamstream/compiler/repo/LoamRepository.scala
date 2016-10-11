package loamstream.compiler.repo

import java.nio.file.Path

import loamstream.loam.LoamScript
import loamstream.util.{Hit, Shot}

/** A repository of Loam scripts */
trait LoamRepository {
  /** Human-readable description on where the scripts are stored */
  def description: String

  /** Lists names of all available Loam scripts */
  def list: Seq[String]

  /** Loads Loam script of given name from repository  */
  def load(name: String): Shot[LoamScript]
}

/** A repository of Loam scripts */
object LoamRepository {
  //TODO: Is this supposed to match what's defined in build.sbt?
  //What happens if we clone to a different dir, like dig-loamstream-foo? 
  val projectName = "dig-loam-stream"
  //TODO: This assumes this code will run from ABT or an IDE; when deployed, 'src/main/resources' won't exist. 
  val resourceFolder = "src/main/resources"
  //TODO: Hard-coding a package name an especially .loam file names feels bad.  Classloaders can't enumerate the
  //contents of a package, but it is possible to look at the classpath and walk down/into each entry.  This is
  //what Spring, JAX-RS, etc do.
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
        //TODO: being tied to a hardcoded SBT project name like this feels a bit bad.  It assumes that we'll be in a
        // subdir of `projectName`, for one thing.
        val classFolderDepth = {
          val indexOfProjectFolder = {
            val parts = (0 until classFolder.getNameCount).map(classFolder.getName).map(_.toString)

            //TODO: What happens when we're not in a subdir of `projectName` and indexOf returns -1? 
            parts.indexOf(projectName)
          }

          classFolder.getNameCount - 1 - indexOfProjectFolder
        }

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
  def withScripts(scripts: Iterable[LoamScript]): LoamMapRepository = LoamMapRepository(scripts)

  /** Creates in-memory repository based on Map */
  def ofMap(entries: Map[String, LoamScript]): LoamMapRepository = LoamMapRepository(entries)

  /** Creates in-memory repository based on Map initially empty */
  def inMemory: LoamMapRepository = withScripts(Nil)

  /** A repository to which Loam scripts can be saved */
  trait Mutable extends LoamRepository {
    /** Saves Loam script to this repository */
    def save(script: LoamScript): Shot[LoamScript]

    /** Creates combo repository combining this and that repository */
    def ++(that: LoamRepository): LoamComboRepository = LoamComboRepository(this, that)
  }

}
