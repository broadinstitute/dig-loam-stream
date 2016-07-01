package loamstream.loam.files

import java.nio.file.{Files, Path}

import loamstream.loam.LoamStore

/**
  * LoamStream
  * Created by oliverr on 6/22/2016.
  */
class LoamFileManager {

  var paths: Map[LoamStore, Path] = Map.empty

  val filePrefix = "loam"

  def getPath(store: LoamStore): Path = {
    paths.get(store).orElse(store.pathOpt) match {
      case Some(path) => path
      case None =>
        val fileSuffix = FileSuffixes(store.sig.tpe)
        val path = Files.createTempFile(filePrefix, s".$fileSuffix")
        paths += store -> path
        path
    }
  }

}
