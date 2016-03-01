package utils

import scala.language.reflectiveCalls

/**
 * Created on: 3/1/16 
 * @author Kaan Yuksel 
 */
object FileUtils {
  def enclosed[C <: { def close() }](c: C)(f: C => Unit) {
    try { f(c) } finally { c.close() }
  }
}
