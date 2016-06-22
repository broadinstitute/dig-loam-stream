package loamstream.compiler

import java.nio.file.Path

import loamstream.util.Shot

/**
  * LoamStream
  * Created by oliverr on 6/1/2016.
  */
object LoamRepository {
  val defaultPackageName = "loam"
  val defaultEntries = Seq("first", "impute")
  val defaultRepo = ofPackage(defaultPackageName, defaultEntries)

  def ofFolder(path: Path): LoamFolderRepository = LoamFolderRepository(path)

  def ofPackage(packageName: String, entries: Seq[String]): LoamPackageRepository =
    LoamPackageRepository(packageName, entries)
}

trait LoamRepository {
  def list: Seq[String]

  def get(name: String): Shot[String]
}
