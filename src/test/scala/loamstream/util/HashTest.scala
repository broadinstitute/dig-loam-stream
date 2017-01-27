package loamstream.util

import java.nio.file.Paths

import org.scalatest.FunSuite

import scala.collection.mutable

/**
  * @author clint
  *         date: Aug 4, 2016
  */
final class HashTest extends FunSuite {
  private val allZeroes =
    Hash(mutable.WrappedArray.make(Array(0.toByte, 0.toByte, 0.toByte, 0.toByte)), HashType.Sha1)

  private val someOnes =
    Hash(mutable.WrappedArray.make(Array(0.toByte, 255.toByte, 255.toByte, 0.toByte)), HashType.Sha1)

  private val realWorld = Hashes.sha1(Paths.get("src/test/resources/for-hashing/foo.txt"))

  test("valueAsBinary64String") {

    assert(allZeroes.valueAsBase64String === "aaaaaa==")

    assert(someOnes.valueAsBase64String === "ap//aa==")

    val expectedRealWorldHash = if (PlatformUtil.isWindows) {
      "kuugk+jlmf99ly+xeuh/mx0cyxg="
    } else {
      "y3i4qsra98i17swj28mqtty7nnu="
    }
    assert(realWorld.valueAsBase64String === expectedRealWorldHash)
  }

  test("toString") {

    assert(allZeroes.toString === "Sha1(aaaaaa==)")

    assert(someOnes.toString === "Sha1(ap//aa==)")

    val expectedRealWorldHash = if (PlatformUtil.isWindows) {
      "Sha1(kuugk+jlmf99ly+xeuh/mx0cyxg=)"
    } else {
      "Sha1(y3i4qsra98i17swj28mqtty7nnu=)"
    }
    assert(realWorld.toString === expectedRealWorldHash)
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
