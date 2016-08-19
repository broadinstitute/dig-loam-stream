package loamstream.util

import java.nio.file.{Path, Paths}
import java.time.Instant

/**
  * LoamStream
  * Created by oliverr on 7/13/2016.
  */
object PathUtils {
  //TODO: TEST!
  def transformFileName(path: Path, transformation: String => String): Path =
    (path.getParent, path.getFileName) match {
      case (null, null) => null // scalastyle:ignore null
      case (null, fileName) => Paths.get(transformation(fileName.toString)) // scalastyle:ignore null
      case (parent, fileName) => parent.resolve(transformation(fileName.toString))
    }

  //TODO: TEST!
  def getFileNameTransformation(nameTransform: String => String): Path => Path = { path =>
    transformFileName(path, nameTransform)
  }

  def lastModifiedTime(p: Path): Instant = {
    val file = p.toFile
    
    val lastModifiedTimeInMillis = if(file.exists) file.lastModified else 0L
    
    Instant.ofEpochMilli(lastModifiedTimeInMillis)
  }
}
