package loamstream.util

import org.scalatest.FunSuite

/** Test of Equivalencer */
class EquivalencesTest extends FunSuite {
  test("Equivalences work es expected") {
    assert(Equivalences.empty[String].theseAreEqual("Hello World!","Hello World!"))
    assert(!Equivalences.empty[String].theseAreEqual("Hello World!","Hi folks!"))
    val equis: Equivalences[String] =
      Equivalences.empty[String].withTheseEqual("Hello", "Hi").withTheseEqual("World", "Welt")
        .withTheseEqual("Welt", "Dunya").withTheseEqual("Hallo", "Merhaba").withTheseEqual("So", "So")
        .withTheseEqual("Hi", "Hallo").withTheseEqual("Merhaba", "Selam")
    val hellos = Set("Hello", "Hi", "Merhaba", "Selam", "Hallo")
    val worlds = Set("World", "Welt", "Dunya")
    assert(equis.equalsOf("Hi") === hellos)
    assert(equis.equalsOf("World") === worlds)
    assert(equis.equalsOf("So") === Set("So"))
    assert(equis.equalsOf("Random") === Set("Random"))
    for(string1 <- hellos) {
      for(string2 <- hellos) {
        assert(equis.theseAreEqual(string1, string2))
      }
    }
    for(string1 <- worlds) {
      for(string2 <- worlds) {
        assert(equis.theseAreEqual(string1, string2))
      }
    }
    for(string1 <- hellos) {
      for(string2 <- worlds + "So" + "Random") {
        assert(!equis.theseAreEqual(string1, string2))
      }
    }
  }
}
