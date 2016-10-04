package loamstream.compiler

import loamstream.loam.LoamScript

/** Class loader that makes sure Loam implementation types are loaded and resolved  */
class LoamClassLoader(val parent: ClassLoader) extends ClassLoader(parent) {

  LoamScript.namesOfNeededSingletons.foreach(loadClass(_, true))

}
