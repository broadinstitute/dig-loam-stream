package loamstream.util

import java.net.URI
import org.scalatest.FunSuite

/**
 * @author kyuksel
 * date: Nov 17, 2016
 */
final class UriEnrichmentsTest extends FunSuite {
  test("appending segments using / to base URIs WITHOUT trailing file separators") {
    import UriEnrichments._
    
    val root = URI.create("/")
    
    val opt = root / "opt"
    
    assert(opt == URI.create("/opt"))
    
    val foo = URI.create("foo")
    
    val fooBarBazTxt = foo / "bar" / "baz.txt"
    
    assert(fooBarBazTxt == URI.create("foo/bar/baz.txt"))
  }
  
  test("appending segments using / to base URIs WITH trailing file separators") {
    import UriEnrichments._

    val root = URI.create("/")

    val opt = root / "opt/"

    assert(opt == URI.create("/opt/"))

    val foo = URI.create("/foo/")

    val fooBarBazTxt = foo / "bar/" / "baz.txt/"

    assert(fooBarBazTxt == URI.create("/foo/bar/baz.txt/"))
  }
}