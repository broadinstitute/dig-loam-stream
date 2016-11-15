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

  val fileSuffix = ".loam"

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
