package loamstream.cli

import org.scalatest.{FunSuite, Matchers}

/**
 * Created by kyuksel on 10/12/16.
 */
final class ConfTest extends FunSuite with Matchers {
  val defaultConfigFile = "src/main/resources/loamstream.conf"

  test("Loam file is parsed correctly") {
    val conf = Conf(Seq("--loam", "src/test/resources/a.txt"))

    conf.loam().toString shouldEqual "src/test/resources/a.txt"
    conf.conf().toString shouldEqual defaultConfigFile
  }

  test("Loam and config files are parsed correctly") {
    val conf = Conf(Seq("--loam", "src/test/resources/a.txt", "--conf", "src/test/resources/loamstream-test.conf"))

    conf.loam().toString shouldEqual "src/test/resources/a.txt"
    conf.conf().toString shouldEqual "src/test/resources/loamstream-test.conf"
  }
}
