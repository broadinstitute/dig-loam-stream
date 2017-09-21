package loamstream.conf

import com.typesafe.config.{ConfigException, ConfigFactory}
import org.scalatest.FunSuite

/**
 * @author kyuksel
 *         date: 5/11/17
 */
final class DataConfigTest extends FunSuite {
  private val conf = DataConfig.fromFile("src/test/resources/data.conf")

  test("isDefined") {
    val data = conf.getObjList("data")
    assert(data(0).isDefined("pops"))
    assert(data(1).isDefined("pops"))

    assert(conf.isDefined("arrays.ex"))
    assert(!conf.isDefined("arrays.dummy"))
    assert(!conf.isDefined("arrays.doesNotExist"))
  }

  test("getStr") {
    assert(conf.getStr("project.id") === "METSIM")

    assert(conf.getStr("binaries.local") === "/humgen/diabetes2/users/someone/software")
    assert(conf.getStr("binaries.binKing") === "/humgen/diabetes2/users/someone/software/king")

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

  test("getBool") {
    val phenotypes = conf.getObjList("phenotypes")

    assert(phenotypes(0).getBool("dichotomous"))
    assert(!phenotypes(1).getBool("dichotomous"))
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
  
  test("fromFile() fails fast if file doesn't exist") {
    //Shouldn't throw
    DataConfig.fromFile("src/test/resources/data.conf")
    
    intercept[Exception] {
      DataConfig.fromFile("foo/bar/baz/blerg")
    }
  }

  private def parse(s: String): DataConfig = DataConfig(ConfigFactory.parseString(s))
}
