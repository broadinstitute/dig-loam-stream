package loamstream.util

import org.scalatest.FunSuite
import java.nio.file.Paths

/**
 * @author clint
 * date: Aug 4, 2016
 */
final class HashTest extends FunSuite {
  private val allZeroes = Hash(Array(0,0,0,0), HashType.Sha1)
  
  private val someOnes = Hash(Array(0,255.toByte,255.toByte,0), HashType.Sha1)
  
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
}