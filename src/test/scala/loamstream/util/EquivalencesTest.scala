package loamstream.util

import org.scalatest.FunSuite

/** Test of Equivalencer */
final class EquivalencesTest extends FunSuite {
  
  test("Nothing is equivalent") {
    assert(Equivalences.empty[String].theseAreEqual("Hello World!", "Hello World!"))
    assert(!Equivalences.empty[String].theseAreEqual("Hello World!", "Hi folks!"))
  }

  private val equis: Equivalences[String] = {
    Equivalences.empty[String].withTheseEqual("Hello", "Hi").withTheseEqual("World", "Welt")
      .withTheseEqual("Welt", "Dunya").withTheseEqual("Hallo", "Merhaba").withTheseEqual("So", "So")
      .withTheseEqual("Hi", "Hallo").withTheseEqual("Merhaba", "Selam")
  }
  
  private val hellos = Set("Hello", "Hi", "Merhaba", "Selam", "Hallo")
  private val worlds = Set("World", "Welt", "Dunya")

  test("Selected equivalences") {
    assert(equis.equalsOf("Hi") === hellos)
    assert(equis.equalsOf("World") === worlds)
    assert(equis.equalsOf("So") === Set("So"))
    assert(equis.equalsOf("Random") === Set("Random"))
    
    for {
      string1 <- hellos
      string2 <- hellos
    } {
      assert(equis.theseAreEqual(string1, string2))
    }
    
    for {
      string1 <- worlds
      string2 <- worlds
    } {
      assert(equis.theseAreEqual(string1, string2))
    }
    
    for {
      string1 <- hellos
      string2 <- worlds + "So" + "Random"
    } {
      assert(!equis.theseAreEqual(string1, string2))
    }
  }

  private val items = hellos ++ worlds

  test("Reflexivity") {
    for (item1 <- items) {
      assert(equis.theseAreEqual(item1, item1))
    }
  }
  
  test("Symmetry") {
    for {
      item1 <- items
      item2 <- items
    } {
      assert(equis.theseAreEqual(item1, item2) == equis.theseAreEqual(item2, item1))
    }
  }
  
  test("Transitiveness") {
    for {
      item1 <- items
      item2 <- items
      item3 <- items
    } {
      val equal12 = equis.theseAreEqual(item1, item2)
      val equal23 = equis.theseAreEqual(item2, item3)
      val equal13 = equis.theseAreEqual(item1, item3)

      assert(!(equal12 && equal23) || equal13)
    }
  }
}
