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

  test("getPathWithoutLeadingSlash") {
    import UriEnrichments._

    val uri1 = URI.create("gs://bucket/data/object")
    val path1 = uri1.getPathWithoutLeadingSlash
    val expectedPath1 = "data/object"

    assert(path1 === expectedPath1)

    val uri2 = URI.create("file://localhost/etc/fstab")
    val path2 = uri2.getPathWithoutLeadingSlash
    val expectedPath2 = "etc/fstab"

    assert(path2 === expectedPath2)

    val uri3 = URI.create("file:///localhost/etc/fstab")
    val path3 = uri3.getPathWithoutLeadingSlash
    val expectedPath3 = "localhost/etc/fstab"

    assert(path3 === expectedPath3)
  }
}
