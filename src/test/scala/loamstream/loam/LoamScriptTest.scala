package loamstream.loam

import org.scalatest.FunSuite
import loamstream.TestHelpers
import scala.util.Try

/**
 * @author clint
 * Jan 9, 2017
 */
final class LoamScriptTest extends FunSuite {
  import LoamScript._
  import TestHelpers.path
  
  test("nameAndEnclosingDirFromFilePath - loam exisits in dir") {
    val a = path("src/test/loam/a.loam")
    
    val shot = nameAndEnclosingDirFromFilePath(a, path("src/test/loam/"))
    
    val expected = Try("a" -> None)
    
    assert(shot === expected)
  }
  
  test("nameAndEnclosingDirFromFilePath - loam exists in subdir") {
    val a = path("src/test/loam/a.loam")
    
    val shot = nameAndEnclosingDirFromFilePath(a, path("src/test/"))
    
    val expected = Try("a" -> Some(path("loam")))
    
    assert(shot === expected)
  }
  
  test("nameAndEnclosingDirFromFilePath - non-loam") {
    val a = path("src/test/loam/a.scala")
    
    assert(nameAndEnclosingDirFromFilePath(a, path("src/test/")).isFailure)
    
    assert(nameAndEnclosingDirFromFilePath(a, path("src/test/loam")).isFailure)
  }
  
  test("nameFromFilePath") {
    val aLoam = path("src/test/loam/a.loam")
    val aScala = path("src/test/loam/a.scala")
    
    assert(nameFromFilePath(aLoam) === Try("a"))
    
    assert(nameFromFilePath(aScala).isFailure)
  }
  
  test(".loam files import java.net.URI") {
    val loamCode = "val u: URI = null"
    
    assert(TestHelpers.compile(loamCode).isValid)
  }
  
  test(".loam files import loamstream.googlecloud.GoogleSupport._") {
    val loamCode0 = "googleCopy(store, store)"
    
    assert(TestHelpers.compile(loamCode0).isValid)
    
    val loamCode1 = "googleCopy(Seq(store, store), Seq(store, store))"
    
    assert(TestHelpers.compile(loamCode1).isValid)
  }
}
