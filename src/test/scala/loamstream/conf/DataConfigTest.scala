package loamstream.conf

import com.typesafe.config.{ConfigException, ConfigFactory}
import org.scalatest.FunSuite

/**
 * @author kyuksel
 *         date: 5/11/17
 */
final class DataConfigTest extends FunSuite {
  private val conf = DataConfig.fromFile("src/test/resources/data.conf")

  test("getStr") {
    assert(conf.getStr("project.id") === "METSIM")

    intercept[ConfigException.Missing] {
      conf.getStr("id.invalidKey")
    }

    intercept[ConfigException.Missing] {
      conf.getStr("invalidPath")
    }
  }

  test("getInt") {
    assert(conf.getInt("project.phase.number") === 1)

    intercept[ConfigException.WrongType] {
      conf.getInt("project.id.phase.number")
    }

    intercept[ConfigException] {
      conf.getInt("id.invalidKey")
    }

    intercept[ConfigException] {
      conf.getInt("invalidPath")
    }
  }

  test("get{Obj,Str}List") {
    assert(conf.getObjList("data")(1).getStr("id") === "OMNI")
    assert(conf.getObjList("data")(0).getStrList("pops") === Seq("EUR", "SAS", "AMR"))
    assert(conf.getObjList("data")(0).getStrList("pops")(2) === "AMR")
    assert(conf.getObjList("data")(1).getStrList("pops") === Seq())
  }

  test("getIntList") {
    // scalastyle:off magic.number

    val validConfig = parse("ints = [1, -42, 987]")
    assert(validConfig.getIntList("ints") === Seq(1, -42, 987))

    val truncatedConfig = parse("ints = [1.05, -3.57]")
    assert(truncatedConfig.getIntList("ints") === Seq(1, -3))

    val invalidConfig1 = parse("ints = [x, -42]")
    intercept[ConfigException.WrongType] {
      invalidConfig1.getIntList("ints")
    }

    val invalidConfig2 = parse("ints = [-42, \"99\"]")
    assert(invalidConfig2.getIntList("ints") === Seq(-42, 99))

    // scalastyle:on magic.number
  }

  private def parse(s: String): DataConfig = DataConfig(ConfigFactory.parseString(s))
}
