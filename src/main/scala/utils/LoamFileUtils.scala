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
  private def classLoader = getClass.getClassLoader

  def resolveRelativePath(relativePath: String): Path = Paths.get(relativePath)

  def printToFile(f: File)(op: PrintWriter => Unit): Unit = {
    val p = new PrintWriter(f)

    LoamFileUtils.enclosed(p)(op)
  }

  def enclosed[C: CanBeClosed](c: C)(f: C => Unit): Unit = {
    try {
      f(c)
    } finally {
      val closer = implicitly[CanBeClosed[C]]

      closer.close(c)
    }
  }
}
