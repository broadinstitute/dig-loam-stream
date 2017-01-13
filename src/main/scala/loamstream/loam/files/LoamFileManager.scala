package loamstream.loam.files

import java.nio.file.{Files, Path}

import loamstream.loam.LoamStore
import loamstream.util.ValueBox
import loamstream.util.BashScript

/**
  * LoamStream
  * Created by oliverr on 6/22/2016.
  */
final class LoamFileManager {

  private[this] val pathsBox: ValueBox[Map[LoamStore.Untyped, Path]] = ValueBox(Map.empty)

  val filePrefix = "loam"

  def getPath(store: LoamStore.Untyped): Path = {
    pathsBox.getAndUpdate { paths =>
      paths.get(store).orElse(store.pathOpt) match {
        case Some(path) => (paths, path)
        case None =>
          val fileSuffix = FileSuffixes(store.sig.tpe)
          val path = Files.createTempFile(filePrefix, s".$fileSuffix")
          
          (paths + (store -> path), path)
      }
    }
  }

  def getStoreString(store: LoamStore.Untyped): String = { 
    val rawString = pathsBox.getAndUpdate { paths =>
      paths.get(store).orElse(store.pathOpt).orElse(store.uriOpt) match {
        case Some(locator) => (paths, locator.toString)
        case None =>
          val fileSuffix = FileSuffixes(store.sig.tpe)
          val path = Files.createTempFile(filePrefix, s".$fileSuffix")
          (paths + (store -> path), path.toString)
      }
    }
      
    BashScript.escapeString(rawString)
  }
}
