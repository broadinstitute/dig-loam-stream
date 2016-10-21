package loamstream.util

import org.scalatest.FunSuite

/**
  * LoamStream
  * Created by oliverr on 10/21/2016.
  */
class TemplaterTest extends FunSuite {
  test("Substitute using [[ and ]] as delimiter") {
    val grassProps = Map("entity" -> "grass", "color" -> "green")
    val templater = Templater("[[", "]]") + ("greeting", "Hello") + ("addressee", "world") ++ grassProps
    val template = "[[greeting]], [[addressee]]! The color of [[entity]] is [[color]]."
    val substituted = templater(template)
    val expected = "Hello, world! The color of grass is green."
    assert(substituted === expected)
  }
  test("Substitute using moustache templater with {{ and }} as delimiter") {
    val grassProps = Map("entity" -> "grass", "color" -> "green")
    val templater = Templater.moustache + ("greeting", "Hello") + ("addressee", "world") ++ grassProps
    val template = "{{greeting}}, {{addressee}}! The color of {{entity}} is {{color}}."
    val substituted = templater(template)
    val expected = "Hello, world! The color of grass is green."
    assert(substituted === expected)
  }
}
