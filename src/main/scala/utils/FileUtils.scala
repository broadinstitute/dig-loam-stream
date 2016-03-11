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

  def printToFile(f: File)(op: PrintWriter => Unit): Unit = {
    val p = new PrintWriter(f)
    
    FileUtils.enclosed(p)(op)
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
