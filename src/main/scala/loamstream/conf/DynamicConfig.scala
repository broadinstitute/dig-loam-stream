package loamstream.conf

import com.typesafe.config.Config

import scala.language.dynamics

import net.ceedubs.ficus.readers.ValueReader

/**
 * @author clint
 * May 2, 2017
 * 
 * A class to allow succinct, but NOT type-safe access to typesafe-config objects.
 * Stores a wrapped Config object, and an optional path into that object.
 */
final case class DynamicConfig(
    private val rootConfig: Config, 
    private val pathOption: Option[String] = None) extends scala.Dynamic {
  
  /**
   * Appends the desired field/method to pathOption's value.
   * 
   * @see the docs to `scala.Dynamic`
   * If a method is invoked on an object of this class that the compiler can't find, that call is translated into
   * an invocation of this method, with the desired method name passed as `fieldName`.  For example, given
   * 
   * val conf = DynamicConfig(...)
   * 
   * conf.foo would get translated into conf.selectDynamic("foo")
   * 
   * Allows chained calls like 
   * 
   * conf.foo.bar.baz
   */
  def selectDynamic(fieldName: String): DynamicConfig = {
    val newPath = pathOption match {
      case None => Some(fieldName)
      case Some(oldPath) => Some(s"${oldPath}.$fieldName")
    }
    
    DynamicConfig(rootConfig, newPath)
  }
  
  /**
   * Return the value obtained by following the path contained by pathOption (if any) into rootConfig, 
   * but only if that value is a boolean, string, or number.  Otherwise, throw.
   * 
   * This restriction exists because it's not clear how to represent a config object or list, each with 
   * possible sub-objects or sub-lists, as a single value that can be turned into a string in a 
   * straightforward way, since this method is intended to be used by interpolators like cmd"...".
   */
  def unpack: Any = {
    require(
        pathOption.isDefined, 
        s"Can't unpack config objects or lists, only numbers, strings, and booleans, tried to unpack $rootConfig")
    
    val path = pathOption.get
    
    require(rootConfig.hasPath(path), s"Couldn't find path $path in $rootConfig ")
    
    val unwrapped = rootConfig.getValue(path).unwrapped
    
    unwrapped match {
      case s: String => s
      case i: java.lang.Integer => i.intValue
      case d: java.lang.Double => d.doubleValue
      case b: java.lang.Boolean => b.booleanValue
      //TODO: :(
      case _ => throw new Exception(s"Only unpacking primitives (ints, doubles, strings, and booleans) is supported.")
    }
  }
  
  /**
   * Return the value obtained by following the path contained by pathOption (if any) into rootConfig, and
   * unmarshalling it into an A, if possible.
   * 
   * @see PythonConfig
   * @see RConfig
   * @see UgerConfig
   * @see ExecutionConfig
   * @see https://github.com/iheartradio/ficus
   */
  def as[A](implicit reader: ValueReader[A]): A = reader.read(rootConfig, pathOption.getOrElse(""))
}

