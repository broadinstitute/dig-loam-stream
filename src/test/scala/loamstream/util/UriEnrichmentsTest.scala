package loamstream.util

import java.net.URI
import org.scalatest.FunSuite

/**
 * @author kyuksel
 * date: Nov 17, 2016
 */
final class UriEnrichmentsTest extends FunSuite {
  import UriEnrichments._

  test("appending segments using / to base URIs WITHOUT trailing file separators") {
    val root = URI.create("/")
    
    val opt = root / "opt"
    
    assert(opt == URI.create("/opt"))
    
    val foo = URI.create("foo")
    
    val fooBarBazTxt = foo / "bar" / "baz.txt"
    
    assert(fooBarBazTxt == URI.create("foo/bar/baz.txt"))
  }
  
  test("appending segments using / to base URIs WITH trailing file separators") {
    val root = URI.create("/")

    val opt = root / "opt/"

    assert(opt == URI.create("/opt/"))

    val foo = URI.create("/foo/")

    val fooBarBazTxt = foo / "bar/" / "baz.txt/"

    assert(fooBarBazTxt == URI.create("/foo/bar/baz.txt/"))
  }

  test("appending strings using + to URIs without trailing file separators") {
    val uriStr = "gs://bucket/data/object"
    val uri = URI.create(uriStr)
    val ext = "txt"
    val expectedUri = URI.create(uriStr + s".$ext")

    assert(uri + s".$ext" === expectedUri)
  }

  test("getPathWithoutLeadingSlash") {
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

  test("lastSegment") {
    val uri1 = URI.create("gs://bucket/data/object")
    val lastSegment1 = uri1.lastSegment
    val expectedLastSegment1 = "object"

    assert(lastSegment1 === expectedLastSegment1)

    val uri2 = URI.create("file://localhost/etc/fstab")
    val lastSegment2 = uri2.lastSegment
    val expectedlastSegment2 = "fstab"

    assert(lastSegment2 === expectedlastSegment2)

    val uri3 = URI.create("gs://bucket/dir/.object")
    val lastSegment3 = uri3.lastSegment
    val expectedlastSegment3 = ".object"

    assert(lastSegment3 === expectedlastSegment3)

    val uri4 = URI.create("gs://bucket/dir/.object?x=5#blah")
    val lastSegment4 = uri4.lastSegment
    val expectedlastSegment4 = ".object"

    assert(lastSegment4 === expectedlastSegment4)
  }
}
