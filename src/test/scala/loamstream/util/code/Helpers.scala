package loamstream.util.code

import scala.reflect.runtime.universe.Symbol
import scala.reflect.runtime.universe.Type
import scala.reflect.runtime.universe.TypeTag
import scala.reflect.runtime.universe.typeOf
import org.scalatest.Assertions

/**
 * @author clint
 * Jan 2, 2018
 */
object Helpers extends Assertions {
  def packageIdFor(dotSeparatedName: String): PackageId = {
    val z: PackageId = RootPackageId
    
    val result = dotSeparatedName.split("\\.").foldLeft(z)(_.getPackage(_))
    
    assert(result.parts.mkString(".") === s"_root_.$dotSeparatedName")
    
    result
  }
  
  def symbolFor[T: TypeTag]: Symbol = typeOf[T].typeSymbol
    
  def owningSymbol[T: TypeTag]: Symbol = symbolFor[T].owner
  
  object Dummy {
    trait Foo
    
    object Blarg {
      trait Glerg
    }
  }
  
  trait Bar
}
