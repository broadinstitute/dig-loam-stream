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
      "da39a3ee5e6b4b0d3255bfef95601890afd80709",
      "cb78b8412adaf7c8b5eecc09dbc9aa4d3cbb3675")

    doTest("src/test/resources/for-hashing/foo.txt",
      "91452093e8cb99ff7d958fb17941ff317d026318",
      "cb78b8412adaf7c8b5eecc09dbc9aa4d3cbb3675")

    doTest("src/test/resources/for-hashing/bigger",
      "cd6f5ff26203054b2284a169c796200bc75f9db5",
      "cd6f5ff26203054b2284a169c796200bc75f9db5")
  }

  test("sha1(File) - Directory") {
    doTest("src/test/resources/for-hashing/subdir/",
      "400a222b7b8a346b95e2db09d6e94c5f09ca1dc5",
      "400a222b7b8a346b95e2db09d6e94c5f09ca1dc5")

    doTest("src/test/resources/for-hashing/subdir/bar.txt",
      "161542ba7efa24cbb0a91eef5064064433d38a40",
      "161542ba7efa24cbb0a91eef5064064433d38a40")

    doTest("src/test/resources/for-hashing/",
      "d7c233c5639c52c7319344d5b210fcd25882ff57",
      "8dc055c24c34679940ff695a285982067e272cc2")
  }

  private def doTest(file: String, expectedOnWindows: String, expectedElsewhere: String): Unit = {
    val hash = Hashes.sha1(path(file))

    assert(hash.tpe == HashType.Sha1)

    if (PlatformUtil.isWindows) {
      assert(hash.valueAsHexString == expectedOnWindows)
    } else {
      assert(hash.valueAsHexString == expectedElsewhere)
    }
  }
}