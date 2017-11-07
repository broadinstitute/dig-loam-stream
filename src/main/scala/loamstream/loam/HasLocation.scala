package loamstream.loam

import java.nio.file.Path
import java.net.URI
import loamstream.loam.files.LoamFileManager
import loamstream.util.TypeBox

/**
 * @author clint
 * Aug 10, 2017
 */
trait HasLocation {
  
  def path: Path 
  
  def pathOpt: Option[Path] = Option(path)
  
  def uriOpt: Option[URI] = None
  
  def render(fileManager: LoamFileManager): String = fileManager.getStoreString(this)
}
