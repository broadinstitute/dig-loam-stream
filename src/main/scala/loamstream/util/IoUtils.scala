package loamstream.util

import java.io.PrintWriter

/**
 * @author clint
 * Feb 27, 2018
 */
object IoUtils {
  def printToConsole(): Unit = println() //scalastyle:ignore regex
  
  def printToConsole(a: Any): Unit = println(a) //scalastyle:ignore regex
  
  def printTo(writer: PrintWriter)(a: Any): Unit = writer.println(a) //scalastyle:ignore regex
}
