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

  private val pathsBox: ValueBox[Map[HasLocation, Path]] = ValueBox(Map.empty)

  val filePrefix = "loam"

  def getPath(store: HasLocation): Path = {
    pathsBox.getAndUpdate { paths =>
      paths.get(store).orElse(store.pathOpt) match {
        case Some(path) => (paths, path)
        case None => {
          //TODO: hard-code this?
          val fileSuffix = "txt"
          val path = Files.createTempFile(filePrefix, s".$fileSuffix")
          
          (paths + (store -> path), path)
        }
      }
    }
  }

  def getStoreString(store: HasLocation): String = {
    val rawString = pathsBox.getAndUpdate { paths =>
      paths.get(store).orElse(store.pathOpt).orElse(store.uriOpt) match {
        case Some(locator) => (paths, locator.toString)
        case None =>
          //TODO: hard-code this?
          val fileSuffix = "txt"
          val path = Files.createTempFile(filePrefix, s".$fileSuffix")
          (paths + (store -> path), path.toString)
      }
    }
    
    BashScript.escapeString(rawString)
  }
}

object LoamFileManager {
  def apply(initial: Map[HasLocation, Path] = Map.empty): LoamFileManager = {
    val result = new LoamFileManager
    
    result.pathsBox := initial
    
    result
  }
}
