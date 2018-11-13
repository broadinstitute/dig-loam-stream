package loamstream.util

import org.scalatest.FunSuite

/**
 * @author clint
 * Nov 7, 2018
 */
final class BiMapTest extends FunSuite {
  test("guards") {
    BiMap()
    BiMap("x" -> 1, "y" -> 2)
    
    intercept[Exception] {
      BiMap("x" -> 1, "x" -> 2)
    }
  }
  
  test("size") {
    assert(BiMap.empty.size === 0)
    assert(BiMap("x" -> 42).size === 1)
    assert(BiMap("x" -> 42, "y" -> 99).size === 2)
    assert(BiMap("x" -> 42, "y" -> 99, "z" -> 123).size === 3)
    assert((BiMap("x" -> 42, "y" -> 99, "z" -> 123) - "y").size === 2)
  }
  
  test("isEmpty") { 
    assert(BiMap.empty.isEmpty)
    assert(BiMap().isEmpty)
    assert(BiMap("x" -> 42).isEmpty === false)
    assert(BiMap("x" -> 42, "y" -> 99).isEmpty === false)
  }
  
  test("inverse") {
    assert(BiMap.empty[Int, String].inverse === BiMap.empty)
    assert(BiMap.empty[Int, String].inverse.inverse === BiMap.empty)
    
    val m2 = BiMap("x" -> 42, "y" -> 99)
    
    assert(m2.inverse.inverse === m2)
    
    assert(m2.inverse === BiMap(42 -> "x", 99 -> "y"))
  }
  
  test("keys") {
    assert(BiMap.empty[Int, Int].keys.isEmpty)
    
    val m2 = BiMap("x" -> 42, "y" -> 99)
    
    assert(m2.keys === Set("x", "y"))
  }
  
  test("values") {
    assert(BiMap.empty[Int, Int].values.isEmpty)
    
    val m2 = BiMap("x" -> 42, "y" -> 99)
    
    assert(m2.values === Set(42, 99))
  }
  
  test("get") {
    val m2 = BiMap("x" -> 42, "y" -> 99)
    
    assert(m2.get("x") === Some(42))
    assert(m2.get("y") === Some(99))
    assert(m2.get("z") === None)
  }
  
  test("getValue") {
    val m2 = BiMap("x" -> 42, "y" -> 99)
    
    assert(m2.getByValue(42) === Some("x"))
    assert(m2.getByValue(99) === Some("y"))
    assert(m2.getByValue(123) === None)
  }
  
  test("contains") {
    val m2 = BiMap("x" -> 42, "y" -> 99)
    
    assert(m2.contains("x"))
    assert(m2.contains("y"))
    assert(m2.contains("z") === false)
  }
  
  test("containsValue") {
    val m2 = BiMap("x" -> 42, "y" -> 99)
    
    assert(m2.containsValue(42))
    assert(m2.containsValue(99))
    assert(m2.containsValue(123) === false)
  }
  
  test("filterKeys") {
    val m3 = BiMap("x" -> 42, "y" -> 99, "z" -> 123)
    
    assert(m3.filterKeys(_ != "z") === BiMap("x" -> 42, "y" -> 99))
    
    assert(m3.filterKeys(_ => true) === m3)
    assert(m3.filterKeys(_ != "asdf") === m3)
  }
  
  test("filterValues") {
    val m3 = BiMap("x" -> 42, "y" -> 99, "z" -> 123)
    
    def isOdd(i: Int) = i % 2 != 0
    
    assert(m3.filterValues(isOdd) === BiMap("y" -> 99, "z" -> 123))
    
    assert(m3.filterValues(_ != 0) === m3)
    assert(m3.filterValues(_ => true) === m3)
  }
  
  test("mapKeys") {
    val m3 = BiMap("x" -> 42, "y" -> 99, "z" -> 123)
    
    assert(m3.mapKeys(_ * 2) === BiMap("xx" -> 42, "yy" -> 99, "zz" -> 123))
  }
  
  test("mapValues") {
    val m3 = BiMap("x" -> 42, "y" -> 99, "z" -> 123)
    
    assert(m3.mapValues(_ + 1) === BiMap("x" -> 43, "y" -> 100, "z" -> 124))
  }
  
  test("++") {
    val m3 = BiMap.empty ++ Seq("x" -> 42, "y" -> 99, "z" -> 123)
    
    assert(m3.toMap === Map("x" -> 42, "y" -> 99, "z" -> 123))
    
    assert((m3 ++ Nil) === m3)
  }
  
  test("--") {
    val m4 = BiMap("x" -> 42, "y" -> 99, "z" -> 123, "w" -> 11)
    
    assert((m4 -- m4.keys) === BiMap.empty)
    assert((m4 -- Nil) === m4)
    //removing keys that aren't in the BiMap does nothing
    assert((m4 -- "abc".map(_.toString)) === m4)
    
    assert(m4 -- Seq("x", "z", "w", "a")=== BiMap("y" -> 99))
  }
  
  test("+") {
    val e = BiMap.empty[String, Int]
    
    val m1 = e + ("x" -> 42)
    
    val m2 = m1 + ("y" -> 99)
    
    val m3 = m2 + ("z" -> 123)
    
    assert(m1 === BiMap("x" -> 42))
    assert(m2 === BiMap("x" -> 42, "y" -> 99))
    assert(m3 === BiMap("x" -> 42, "y" -> 99, "z" -> 123))
  }
  
  test("-") {
    val m3 = BiMap("x" -> 42, "y" -> 99, "z" -> 123)
    
    val m2 = m3 - "z"
    
    val m1 = m2 - "y"
    
    val e = m1 - "x"
    
    assert(e.isEmpty)
    assert(m1 === BiMap("x" -> 42))
    assert(m2 === BiMap("x" -> 42, "y" -> 99))
    assert(m3 === BiMap("x" -> 42, "y" -> 99, "z" -> 123))
  }
  
  test("withoutValue") {
    val m3 = BiMap("x" -> 42, "y" -> 99, "z" -> 123)
    
    val m2 = m3.withoutValue(123)
    
    val m1 = m2.withoutValue(99)
    
    val e = m1.withoutValue(42)
    
    assert(e.isEmpty)
    assert(m1 === BiMap("x" -> 42))
    assert(m2 === BiMap("x" -> 42, "y" -> 99))
    assert(m3 === BiMap("x" -> 42, "y" -> 99, "z" -> 123))
  }
  
  test("empty") {
    val e = BiMap.empty[Int, Float]
    
    assert(e.isEmpty)
    assert(e.size === 0)
    assert(e === BiMap())
    assert(e.forward.isEmpty)
    assert(e.backward.isEmpty)
  }
  
  test("apply") {
    assert(BiMap[Int, String]() === BiMap.empty)
    
    val m = BiMap(42 -> "x", 99 -> "y")
    
    assert(m.isEmpty === false)
    assert(m.size === 2)
    assert(m.forward === Map(42 -> "x", 99 -> "y"))
    assert(m.backward === Map("x" -> 42, "y" -> 99))
  }
  
  test("identity mapping") {
    val m = BiMap(1 -> 1, 42 -> 42, 123 -> 123)
    
    assert(m === m.inverse)
    assert(m === m.inverse.inverse)
    
    assert(m.get(1) === Some(1))
    assert(m.getByValue(1) === Some(1))
    assert(m.get(42) === Some(42))
    assert(m.getByValue(42) === Some(42))
    assert(m.get(123) === Some(123))
    assert(m.getByValue(123) === Some(123))
    
    assert((m - 42) === BiMap(1 -> 1, 123 -> 123))
    assert((m - 42 + (99 -> 99)) === BiMap(1 -> 1, 99 -> 99, 123 -> 123))
  }
  
  test("toMap") {
    val m = BiMap(42 -> "x", 99 -> "y")
    
    assert(m.toMap === Map(42 -> "x", 99 -> "y"))
  }
  
  test("Soundness is preserved by + and ++") {
    val m = BiMap("x" -> 42, "y" -> 99)
    
    val plusOne = m + ("x" -> 123)
    
    assert(plusOne.forward === Map("x" -> 123, "y" -> 99))
    assert(plusOne.backward === Map(123 -> "x", 99 -> "y"))
    
    val m2 = m ++ Seq("x" -> 123, "x" -> 11, "z" -> 17)
    
    assert(m2.forward === Map("x" -> 11, "y" -> 99, "z" -> 17))
    assert(m2.backward === Map(11 -> "x", 99 -> "y", 17 -> "z"))
  }
}
