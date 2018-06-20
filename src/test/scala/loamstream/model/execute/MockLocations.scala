package loamstream.model.execute

import java.nio.file.Path

/**
 * @author clint
 * Jun 20, 2018
 */
object MockLocations {
  def fromPaths(inHostValue: => Path = ???, inContainerValue: => Path = ???): Locations[Path] = new Locations[Path] {
    override def inHost(p: Path): Path = inHostValue 
  
    override def inContainer(p: Path): Path = inContainerValue
  }
  
  def fromFunctions(
      makeInHost: Path => Path = _ => ???, 
      makeContainerValue: Path => Path = _ => ???): Locations[Path] = new Locations[Path] {
    
    override def inHost(p: Path): Path = makeInHost(p) 
  
    override def inContainer(p: Path): Path = makeContainerValue(p)
  } 
}
