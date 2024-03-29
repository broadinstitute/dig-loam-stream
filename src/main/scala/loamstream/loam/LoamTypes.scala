package loamstream.loam

/**
 * @author clint
 * Jul 31, 2019
 */
trait LoamTypes {
  type Path = java.nio.file.Path
  type URI = java.net.URI
  
  type Store = loamstream.model.Store
  type Tool = loamstream.model.Tool
}
