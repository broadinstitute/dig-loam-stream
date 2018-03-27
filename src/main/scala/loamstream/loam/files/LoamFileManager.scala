package loamstream.loam.files

import java.nio.file.{Files, Path}
import java.net.URI
import loamstream.model.Store
import loamstream.util.{BashScript, ValueBox}
import loamstream.loam.HasLocation
import loamstream.conf.ExecutionConfig

/**
  * LoamStream
  * Created by oliverr on 6/22/2016.
  */
final class LoamFileManager(executionConfig: ExecutionConfig) {
  import BashScript.Implicits._
  import LoamFileManager.tempPath
  
  private val pathsBox: ValueBox[Map[HasLocation, Path]] = ValueBox(Map.empty)
  
  def getPath(store: HasLocation): Path = {
    pathsBox.getAndUpdate { paths =>
      paths.get(store).orElse(store.pathOpt) match {
        case Some(path) => (paths, path)
        case None       => tempPath(executionConfig, paths, store)
      }
    }
  }

  def getStoreString(store: HasLocation): String = {
    pathsBox.getAndUpdate { paths =>
      paths.get(store).orElse(store.pathOpt).orElse(store.uriOpt) match {
        case Some(loc: Path) => (paths, loc.render)

        // if it's a URI then we can't shouldn't replace the path separator
        case Some(loc: URI) => (paths, loc.toString)

        // create a temp file
        case _ => {
          val (newPaths, path) = tempPath(executionConfig, paths, store)
          
          (newPaths, path.render)
        }
      }
    }
  }
}

object LoamFileManager {
  def apply(executionConfig: ExecutionConfig, initial: Map[HasLocation, Path] = Map.empty): LoamFileManager = {
    val result = new LoamFileManager(executionConfig)

    result.pathsBox := initial

    result
  }
  
  private[files] def tempPath(
      executionConfig: ExecutionConfig,
      paths: Map[HasLocation, Path], 
      store: HasLocation): (Map[HasLocation, Path], Path) = {
    
    val path = Files.createTempFile(executionConfig.anonStoreDir, "loam", ".txt")

    // updated paths and resulting file path
    (paths + (store -> path), path)
  }
}
