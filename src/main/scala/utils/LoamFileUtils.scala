package utils

import java.io.{File, PrintWriter}
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.Files

/**
  * Created on: 3/1/16
  *
  * @author Kaan Yuksel
  */
object LoamFileUtils {
  def resolveRelativePath(relativePath: String): Path = Paths.get(relativePath)

  def printToFile(f: File)(op: PrintWriter => Unit): Unit = {
    enclosed(new PrintWriter(f))(op)
  }

  //TODO: Move this somewhere else, since it isn't file-specific anymore
  def enclosed[A, C: CanBeClosed](c: C)(f: C => A): A = {
    try {
      f(c)
    } finally {
      val closer = implicitly[CanBeClosed[C]]

      closer.close(c)
    }
  }
}
