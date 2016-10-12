package loamstream.cli

import loamstream.TestHelpers
import org.scalatest.{FunSuite, Matchers}

/**
 * Created by kyuksel on 10/12/16.
 */
final class ConfTest extends FunSuite with Matchers {
  val helpText = """LoamStream is a genomic analysis stack featuring a high-level language, compiler and runtime engine.
                   |Usage: scala loamstream.jar [Option]...
                   |Options:
                   |
                   |  -c, --conf  <arg>   Path to config file
                   |  -l, --loam  <arg>   Path to loam script
                   |      --help          Show help message"""

  val defaultConfigFile = "src/main/resources/loamstream.conf"

  test("When no option is provided, help menu is displayed") {
    val (out, _) = TestHelpers.captureOutput {
      Conf()
    }

    out shouldEqual helpText
  }

  test("When help option is provided, help menu is displayed") {
    val (out, _) = TestHelpers.captureOutput {
      Conf(Seq("--help"))
    }

    out shouldEqual helpText
  }

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

  test("When a loam is provided, check if a config file is also") {
    val (_, err) = TestHelpers.captureOutput {
      Conf(Seq("--conf", "someConf"))
    }

    err shouldEqual "When specifying 'conf', at least one of the following options must be provided: loam"
  }
}
