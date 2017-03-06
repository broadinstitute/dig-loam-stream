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
      "2jmj7l5rSw0yVb/vlWAYkK/YBwk=",
      "2jmj7l5rSw0yVb/vlWAYkK/YBwk=")

    doTest("src/test/resources/for-hashing/foo.txt",
      "kuugk+jlmf99ly+xeuh/mx0cyxg=",
      "y3i4QSra98i17swJ28mqTTy7NnU=")

    doTest("src/test/resources/for-hashing/bigger",
      "zW9f8mIDBUsihKFpx5YgC8dfnbU=",
      "zW9f8mIDBUsihKFpx5YgC8dfnbU=")
  }

  test("sha1(File) - Directory") {
    doTest("src/test/resources/for-hashing/subdir/",
      "QAoiK3uKNGuV4tsJ1ulMXwnKHcU=",
      "QAoiK3uKNGuV4tsJ1ulMXwnKHcU=")

    doTest("src/test/resources/for-hashing/subdir/bar.txt",
      "FhVCun76JMuwqR7vUGQGRDPTikA=",
      "FhVCun76JMuwqR7vUGQGRDPTikA=")

    doTest("src/test/resources/for-hashing/",
      "18izxwocuscxk0tvshd80lic/1c=",
      "jcBVwkw0Z5lA/2laKFmCBn4nLMI=")
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
