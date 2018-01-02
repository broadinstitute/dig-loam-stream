package loamstream.util.code

import org.scalatest.FunSuite

import scala.reflect.runtime.universe.TypeTag
import scala.reflect.runtime.universe.typeOf

/**
 * @author clint
 * Jan 2, 2018
 */
final class PackageIdTest extends FunSuite {
  import Helpers.packageIdFor
  
  private def packageIdOfPackageContaining[T: TypeTag]: PackageId = PackageId.from(typeOf[T].typeSymbol.owner)

  test("from(Symbol) - type NOT in root package") {
    assert(packageIdOfPackageContaining[String] === packageIdFor("java.lang"))
    
    assert(packageIdOfPackageContaining[PackageIdTest] === packageIdFor("loamstream.util.code"))
  }
  
  test("from(Symbol) throws when given non-package") {
    intercept[Exception] {
      PackageId.from(Helpers.symbolFor[String])
    }
    
    intercept[Exception] {
      PackageId.from(Helpers.symbolFor[PackageIdTest])
    }
  }
}
