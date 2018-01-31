package loamstream.loam.files

import java.nio.file.{Files, Path}

import loamstream.model.Store
import loamstream.util.{BashScript, ValueBox}
import loamstream.loam.HasLocation

/**
  * LoamStream
  * Created by oliverr on 6/22/2016.
  */
final class LoamFileManager {
  import BashScript.Implicits._

  private val pathsBox: ValueBox[Map[HasLocation, Path]] = ValueBox(Map.empty)

  val filePrefix = "loam"

  def tempPath(paths: Map[HasLocation, Path], store: HasLocation, fileSuffix: String = "txt") = {
    val path = Files.createTempFile(filePrefix, s".$fileSuffix")

    // updated paths and resulting file path
    (paths + (store -> path), path)
  }

  def getPath(store: HasLocation): Path = {
    pathsBox.getAndUpdate { paths =>
      paths.get(store).orElse(store.pathOpt) match {
        case Some(path) => (paths, path)
        case None       => tempPath(paths, store)
      }
    }
  }

  def getStoreString(store: HasLocation): String = {
    pathsBox.getAndUpdate { paths =>
      paths.get(store).orElse(store.pathOpt) match {
        case Some(loc) => (paths, loc.render)

        // if it's a URI then we can't shouldn't replace the path separator
        case None => store.uriOpt match {
          case Some(loc) => (paths, loc.toString)
          case None      =>
            val (newPaths, path) = tempPath(paths, store)
            (newPaths, path.render)
        }
      }
    }
  }
}

object LoamFileManager {
  def apply(initial: Map[HasLocation, Path] = Map.empty): LoamFileManager = {
    val result = new LoamFileManager

    result.pathsBox := initial

    result
  }
}
