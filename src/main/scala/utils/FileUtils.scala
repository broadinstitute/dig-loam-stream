package utils

import java.io.{File, PrintWriter}
import java.nio.file.Path

import scala.language.reflectiveCalls

/**
  * Created on: 3/1/16
  *
  * @author Kaan Yuksel
  */
object FileUtils {
  def resolveRelativePath(relativePath: String): Path = new File(getClass.getResource(relativePath).toURI).toPath

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
