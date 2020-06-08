package loamstream.util

import org.scalatest.FunSuite

import scala.collection.immutable.ArraySeq

import loamstream.TestHelpers

/**
  * @author clint
  *         date: Aug 4, 2016
  */
final class HashTest extends FunSuite {
  import TestHelpers.path
  
  private val allZeroes = Hash(ArraySeq(0.toByte, 0.toByte, 0.toByte, 0.toByte), HashType.Sha1)

  private val someOnes = Hash(ArraySeq(0.toByte, 255.toByte, 255.toByte, 0.toByte), HashType.Sha1)

  private val realWorld = Hashes.sha1(path("src/test/resources/for-hashing/foo.txt"))

  test("valueAsBinary64String") {

    assert(allZeroes.valueAsBase64String === "AAAAAA==")

    assert(someOnes.valueAsBase64String === "AP//AA==")

    val expectedRealWorldHash = if (PlatformUtil.isWindows) {
      "kUUgk+jLmf99lY+xeUH/MX0CYxg="
    } else {
      "y3i4QSra98i17swJ28mqTTy7NnU="
    }
    assert(realWorld.valueAsBase64String === expectedRealWorldHash)
  }

  test("valueAsBase64String round trip") {
    val deserialized = Hash.fromStrings(Some(someOnes.valueAsBase64String), someOnes.tpe.algorithmName).get
    
    assert(deserialized === someOnes)
    assert(someOnes === deserialized)
  }
  
  test("toString") {

    assert(allZeroes.toString === "Sha1(AAAAAA==)")

    assert(someOnes.toString === "Sha1(AP//AA==)")

    val expectedRealWorldHash = if (PlatformUtil.isWindows) {
      "Sha1(kUUgk+jLmf99lY+xeUH/MX0CYxg=)"
    } else {
      "Sha1(y3i4QSra98i17swJ28mqTTy7NnU=)"
    }
    assert(realWorld.toString === expectedRealWorldHash)
  }

  test("equals") {
    val h1 = Hashes.sha1(path("src/test/resources/for-hashing/foo.txt"))
    val h2 = Hashes.sha1(path("src/test/resources/for-hashing/foo.txt"))

    assert(h1 == h2)
    assert(h2 == h1)

    assert(h1 == realWorld)
    assert(realWorld == h1)

    assert(realWorld == h2)
    assert(h2 == realWorld)

    val h3 = Hashes.sha1(path("src/test/resources/for-hashing/empty.txt"))

    assert(h1 != h3)
    assert(h2 != h3)
    assert(realWorld != h3)
  }
}
