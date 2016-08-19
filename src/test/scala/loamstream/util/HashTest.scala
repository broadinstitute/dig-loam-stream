package loamstream.util

import java.nio.file.Paths

import org.scalatest.FunSuite

import scala.collection.mutable

/**
 * @author clint
 * date: Aug 4, 2016
 */
final class HashTest extends FunSuite {
  private val allZeroes = Hash(mutable.WrappedArray.make(Array(0.toByte,0.toByte,0.toByte,0.toByte)), HashType.Sha1)
  
  private val someOnes =
    Hash(mutable.WrappedArray.make(Array(0.toByte,255.toByte,255.toByte,0.toByte)), HashType.Sha1)
  
  private val realWorld = Hashes.sha1(Paths.get("src/test/resources/for-hashing/foo.txt"))
  
  test("valueAsHexString") {
    
    assert(allZeroes.valueAsHexString == "00000000")
    
    assert(someOnes.valueAsHexString == "00ffff00")
    
    assert(realWorld.valueAsHexString == "cb78b8412adaf7c8b5eecc09dbc9aa4d3cbb3675")
  }
  
  test("toString") {

    assert(allZeroes.toString == "Sha1(00000000)")
    
    assert(someOnes.toString == "Sha1(00ffff00)")
    
    assert(realWorld.toString == "Sha1(cb78b8412adaf7c8b5eecc09dbc9aa4d3cbb3675)")
  }
  
  test("equals") {
    val h1 = Hashes.sha1(Paths.get("src/test/resources/for-hashing/foo.txt"))
    val h2 = Hashes.sha1(Paths.get("src/test/resources/for-hashing/foo.txt"))
    
    assert(h1 == h2)
    assert(h2 == h1)
    
    assert(h1 == realWorld)
    assert(realWorld == h1)

    assert(realWorld == h2)
    assert(h2 == realWorld)
    
    val h3 = Hashes.sha1(Paths.get("src/test/resources/for-hashing/empty.txt"))
    
    assert(h1 != h3)
    assert(h2 != h3)
    assert(realWorld != h3)
  }
}