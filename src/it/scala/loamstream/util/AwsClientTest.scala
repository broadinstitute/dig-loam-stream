package loamstream.util

import org.scalatest.FunSuite

/**
 * @author clint
 * Dec 8, 2020
 */
final class AwsClientTest extends AwsFunSuite {
  private case class Keys(private val testDir: String) {
    val foo = s"${testDir}/foo.txt"
    val bar =s"${testDir}/bar.txt"
    val baz = s"${testDir}/baz.txt"
    val blah = s"${testDir}/subdir/blah.txt"
    val blerg = s"${testDir}/subdir/blerg.txt"
    val zerg = s"${testDir}/subdir/subsubdir/zerg.txt"
    
    def all: Set[String] = Set(foo, bar, baz, blah, blerg, zerg)
  }
  
  test("bucket") {
    val client = newS3Client
    
    assert(client.bucket === "dig-integration-tests")
  }
  
  testWithPseudoDir(s"${getClass.getSimpleName}-put_getAsString_list") { testDir =>
    val client = newS3Client
      
    assert(client.list(testDir) === Nil)
    
    val keys = Keys(testDir)
    
    import keys._
    
    client.put(foo, "FOO", None)
    client.put(bar, "BAR", None)
    client.put(baz, "BAZ", None)
    
    client.put(blah, "BLAH", None)
    client.put(blerg, "BLERG", None)
    client.put(zerg, "ZERG", None)
    
    val expected0 = keys.all
        
    assert(client.list(testDir).toSet === expected0)
    
    assert(client.getAsString(s"${testDir}/") === None)
    assert(client.getAsString(s"${testDir}/lalala") === None)
    
    assert(client.getAsString(foo) === Some("FOO"))
    assert(client.getAsString(bar) === Some("BAR"))
    assert(client.getAsString(baz) === Some("BAZ"))
    assert(client.getAsString(blah) === Some("BLAH"))
    assert(client.getAsString(blerg) === Some("BLERG"))
    assert(client.getAsString(zerg) === Some("ZERG"))
  }
  
  testWithPseudoDir(s"${getClass.getSimpleName}-deleteDir-bit-by-bit") { testDir =>
    val client = newS3Client
      
    assert(client.list(testDir) === Nil)
    
    val keys = Keys(testDir)
    
    import keys._
    
    client.put(foo, "FOO", None)
    client.put(bar, "BAR", None)
    client.put(baz, "BAZ", None)
    
    client.put(blah, "BLAH", None)
    client.put(blerg, "BLERG", None)
    client.put(zerg, "ZERG", None)
    
    val expected0 = keys.all
        
    assert(client.list(testDir).toSet === expected0)
    
    client.deleteDir(s"${testDir}/subdir/subsubdir/")
    
    assert(client.list(testDir).toSet === (expected0 - keys.zerg))
    
    client.deleteDir(s"${testDir}/subdir/")
    
    assert(client.list(testDir).toSet === (expected0 - zerg - blah - blerg))
    
    client.deleteDir(testDir)
    
    assert(client.list(testDir) === Nil)
  }
  
  testWithPseudoDir(s"${getClass.getSimpleName}-deleteDir-all-at-once") { testDir =>
    val client = newS3Client
      
    assert(client.list(testDir) === Nil)
    
    val keys = Keys(testDir)
    
    import keys._
    
    client.put(foo, "FOO", None)
    client.put(bar, "BAR", None)
    client.put(baz, "BAZ", None)
    
    client.put(blah, "BLAH", None)
    client.put(blerg, "BLERG", None)
    client.put(zerg, "ZERG", None)
    
    val expected0 = keys.all
        
    assert(client.list(testDir).toSet === expected0)
    
    client.deleteDir(testDir)

    assert(client.list(testDir) === Nil)
  }
}
