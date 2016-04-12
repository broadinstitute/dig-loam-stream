package utils

import java.nio.file.{Paths, Path}
import scala.util.Try

/**
 * Created on: 3/1/16 
 * @author Kaan Yuksel 
 */
object StringUtils {
  //TODO: TEST!
  def pathTemplate(template: String, slot: String): String => Path = {
    id => Paths.get(template.replace(slot, id))
  }
  
  def isWhitespace(s: String): Boolean = s.trim.isEmpty
  
  def isNotWhitespace(s: String): Boolean = !isWhitespace(s)
  
  object IsLong {
    def unapply(s: String): Option[Long] = Try(s.toLong).toOption
  }
}
