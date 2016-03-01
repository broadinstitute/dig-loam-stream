package utils

import java.nio.file.{Paths, Path}

/**
 * Created on: 3/1/16 
 * @author Kaan Yuksel 
 */
object StringUtils {
  def pathTemplate(template: String, slot: String): String => Path = {
    id => Paths.get(template.replace(slot, id))
  }
}
