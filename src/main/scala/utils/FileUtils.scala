package utils

import java.io.{File, PrintWriter}
import java.nio.file.Path

/**
  * Created on: 3/1/16
  *
  * @author Kaan Yuksel
  */
object FileUtils {
  private def classLoader = getClass.getClassLoader
  
  def resolveRelativePath(relativePath: String): Option[Path] = {
    for {
      resource <- Option(classLoader.getResource(relativePath))
      uri = resource.toURI
      file = new File(uri)
    } yield file.toPath
  }

  def printToFile(f: File)(op: PrintWriter => Unit) {
    val p = new PrintWriter(f)
    FileUtils.enclosed(p)(p.close)(op)
  }

  def enclosed[C](c: C)(closeOp: () => Unit)(f: C => Unit) {
    try {
      f(c)
    } finally {
      closeOp()
    }
  }
}
