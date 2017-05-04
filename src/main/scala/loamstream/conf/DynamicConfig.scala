package loamstream.conf

import com.typesafe.config.Config

import scala.language.dynamics

import com.typesafe.config.ConfigFactory
import com.typesafe.config.ConfigValue
import com.typesafe.config.ConfigObject
import com.typesafe.config.impl.ConfigInt
import com.typesafe.config.ConfigValueType
import net.ceedubs.ficus.readers.ValueReader

/**
 * @author clint
 * May 2, 2017
 */
final case class DynamicConfig(
    private val rootConfig: Config, 
    private val pathOption: Option[String] = None) extends scala.Dynamic {
  
  def selectDynamic(fieldName: String): DynamicConfig = {
    val newPath = pathOption match {
      case None => Some(fieldName)
      case Some(oldPath) => Some(s"${oldPath}.$fieldName")
    }
    
    DynamicConfig(rootConfig, newPath)
  }
  
  def unpack: Option[Any] = {
    val unwrapped = pathOption.flatMap { path =>
      if(rootConfig.hasPath(path)) Option(rootConfig.getValue(path).unwrapped) else None
    }
    
    unwrapped.map {
      case s: String => s
      case i: java.lang.Integer => i.intValue
      case d: java.lang.Double => d.doubleValue
      case b: java.lang.Boolean => b.booleanValue
      //TODO: :(
      case _ => throw new Exception(s"Only unpacking primitives (ints, doubles, strings, and booleans) is supported.")
    }
  }
  
  def as[A](implicit reader: ValueReader[A]): A = reader.read(rootConfig, pathOption.getOrElse(""))
}

