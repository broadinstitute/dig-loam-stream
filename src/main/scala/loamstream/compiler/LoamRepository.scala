package loamstream.compiler

import java.nio.file.Path

import loamstream.util.Shot

/**
  * LoamStream
  * Created by oliverr on 6/1/2016.
  */
object LoamRepository {
  def ofFolder(path: Path): LoamFolderRepository = LoamFolderRepository(path)

  def ofPackage(packageName: String): LoamPackageRepository = LoamPackageRepository(packageName)
}

trait LoamRepository {
  def get(name: String): Shot[String]
}
