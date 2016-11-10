package loamstream.loam.files

import java.nio.file.{Files, Path}

import loamstream.loam.LoamStore

/**
  * LoamStream
  * Created by oliverr on 6/22/2016.
  */
final class LoamFileManager {

  private[this] var paths: Map[LoamStore.Untyped, Path] = Map.empty

  private[this] val lock = new AnyRef
  
  val filePrefix = "loam"

  def getPath(store: LoamStore.Untyped): Path = lock.synchronized {
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
