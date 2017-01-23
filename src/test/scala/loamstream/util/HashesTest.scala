package loamstream.util

import java.nio.file.{Path, Paths}

import org.scalatest.FunSuite

/**
  * @author clint
  *         date: Aug 4, 2016
  */
final class HashesTest extends FunSuite {
  private def path(s: String): Path = Paths.get(s)

  test("sha1(File) - bad input") {
    intercept[Exception] {
      Hashes.sha1(null: Path) //scalastyle:ignore
    }

    intercept[Exception] {
      //Doesn't exist
      Hashes.sha1(path("src/test/resources/laskdjlasdkj"))
    }
  }

  test("sha1(File)") {
    doTest("src/test/resources/for-hashing/empty.txt",
      "2jmj7l5rsw0yvb/vlwaykk/ybwk=",
      "2jmj7l5rsw0yvb/vlwaykk/ybwk=")

    doTest("src/test/resources/for-hashing/foo.txt",
      "91452093e8cb99ff7d958fb17941ff317d026318",
      "y3i4qsra98i17swj28mqtty7nnu=")

    doTest("src/test/resources/for-hashing/bigger",
      "zw9f8midbusihkfpx5ygc8dfnbu=",
      "zw9f8midbusihkfpx5ygc8dfnbu=")
  }

  test("sha1(File) - Directory") {
    doTest("src/test/resources/for-hashing/subdir/",
      "qaoik3uknguv4tsj1ulmxwnkhcu=",
      "qaoik3uknguv4tsj1ulmxwnkhcu=")

    doTest("src/test/resources/for-hashing/subdir/bar.txt",
      "fhvcun76jmuwqr7vugqgrdptika=",
      "fhvcun76jmuwqr7vugqgrdptika=")

    doTest("src/test/resources/for-hashing/",
      "d7c233c5639c52c7319344d5b210fcd25882ff57",
      "jcbvwkw0z5la/2lakfmcbn4nlmi=")
  }

  private def doTest(file: String, expectedOnWindows: String, expectedElsewhere: String): Unit = {
    val hash = Hashes.sha1(path(file))

    assert(hash.tpe == HashType.Sha1)

    if (PlatformUtil.isWindows) {
      assert(hash.valueAsBase64String == expectedOnWindows)
    } else {
      assert(hash.valueAsBase64String == expectedElsewhere)
    }
  }
}
