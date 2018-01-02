package loamstream.util.code

import org.scalatest.FunSuite

/**
  * LoamStream
  * Created by oliverr on 9/16/2016.
  */
final class ScalaIdTest extends FunSuite {
  def assertAllNames(id: ScalaId, inScala: String, inJvm: String, inScalaFull: String, inJvmFull: String): Unit = {
    assert(id.inScala === inScala)
    assert(id.inJvm === inJvm)
    assert(id.inScalaFull === inScalaFull)
    assert(id.inJvmFull === inJvmFull)
  }

  test("from - package") {
    import Helpers.packageIdFor
    import Helpers.owningSymbol
    
    assert(ScalaId.from(owningSymbol[String]) === packageIdFor("java.lang"))
    
    assert(ScalaId.from(owningSymbol[ScalaIdTest]) === packageIdFor("loamstream.util.code"))
  }
  
  test("from - object") {
    import Helpers.packageIdFor
    import Helpers.symbolFor
    
    assert(ScalaId.from(symbolFor[Helpers.type]) === packageIdFor("loamstream.util.code").getObject("Helpers"))
  }
  
  test("from - non-object type") {
    import Helpers.packageIdFor
    import Helpers.symbolFor
    
    assert(ScalaId.from(symbolFor[ScalaIdTest]) === packageIdFor("loamstream.util.code").getType("ScalaIdTest"))
  }

  test("from - object type nested in an object") {
    import Helpers.packageIdFor
    import Helpers.symbolFor
    
    val actual = ScalaId.from(symbolFor[Helpers.Dummy.type])
    
    val expected = packageIdFor("loamstream.util.code").getObject("Helpers").getObject("Dummy")
    
    assert(actual === expected)
  }
  
  test("from - non-object type nested in an object") {
    import Helpers.packageIdFor
    import Helpers.symbolFor
    
    val actual = ScalaId.from(symbolFor[Helpers.Bar])
    
    val expected = packageIdFor("loamstream.util.code").getObject("Helpers").getType("Bar")
    
    assert(actual === expected)
  }
  
  test("from - non-object type nested 2 objects deep") {
    import Helpers.packageIdFor
    import Helpers.symbolFor
    
    val actual = ScalaId.from(symbolFor[Helpers.Dummy.Foo])
    
    val expected = {
      packageIdFor("loamstream.util.code").getObject("Helpers").getObject("Dummy").getType("Foo")
    }
    
    assert(actual === expected)
  }
  
  test("from - non-object type nested 3 objects deep") {
    import Helpers.packageIdFor
    import Helpers.symbolFor
    
    val actual = ScalaId.from(symbolFor[Helpers.Dummy.Blarg.Glerg])
    
    val expected = {
      packageIdFor("loamstream.util.code").getObject("Helpers").getObject("Dummy").getObject("Blarg").getType("Glerg")
    }
    
    assert(actual === expected)
  }
  
  test("from - object type nested 2 objects deep") {
    import Helpers.packageIdFor
    import Helpers.symbolFor
    
    val actual = ScalaId.from(symbolFor[Helpers.Dummy.Blarg.type])
    
    val expected = {
      packageIdFor("loamstream.util.code").getObject("Helpers").getObject("Dummy").getObject("Blarg")
    }
    
    assert(actual === expected)
  }
  
  test("Root package") {
    assertAllNames(RootPackageId, "_root_", "_root_", "_root_", "_root_")
  }

  test("Top level Java id package") {
    assertAllNames(PackageId("test"), "test", "test", "test", "test")
    assertAllNames(TypeId("Test"), "Test", "Test", "Test", "Test")
    assertAllNames(ObjectId("Test"), "Test", "Test$", "Test", "Test$")
  }

  test("Top level non-Java id package.") {
    val name = "Hello, World!"
    val nameWithBackticks = "`Hello, World!`"
    val nameEncoded = "Hello$u002C$u0020World$bang"
    val objectNameEncoded = "Hello$u002C$u0020World$bang$"
    assertAllNames(PackageId(name), nameWithBackticks, nameEncoded, nameWithBackticks, nameEncoded)
    assertAllNames(TypeId(name), nameWithBackticks, nameEncoded, nameWithBackticks, nameEncoded)
    assertAllNames(ObjectId(name), nameWithBackticks, objectNameEncoded, nameWithBackticks, objectNameEncoded)
  }

  test("Java id in package") {
    val parent = PackageId("a", "b", "c")
    assertAllNames(PackageId(parent, "test"), "test", "test", "a.b.c.test", "a.b.c.test")
    assertAllNames(TypeId(parent, "test"), "test", "test", "a.b.c.test", "a.b.c.test")
    assertAllNames(ObjectId(parent, "test"), "test", "test$", "a.b.c.test", "a.b.c.test$")
  }

  test("Non-Java id in package") {
    val parent = PackageId("a", "b", "c")
    val prefix = "a.b.c."
    val name = "Hello, World!"
    val nameWithBackticks = "`Hello, World!`"
    val nameEncoded = "Hello$u002C$u0020World$bang"
    val objectNameEncoded = "Hello$u002C$u0020World$bang$"
    assertAllNames(PackageId(parent, name), nameWithBackticks, nameEncoded,
      prefix + nameWithBackticks, prefix + nameEncoded)
    assertAllNames(TypeId(parent, name), nameWithBackticks, nameEncoded,
      prefix + nameWithBackticks, prefix + nameEncoded)
    assertAllNames(ObjectId(parent, name), nameWithBackticks, objectNameEncoded,
      prefix + nameWithBackticks, prefix + objectNameEncoded)
  }

  test("Non-Java id and non-Java package name") {
    val parent = PackageId("+", "42", " ")
    val prefixScala = "`+`.`42`.` `."
    val prefixJvm = "$plus.42.$u0020."
    val name = "Hello, World!"
    val nameWithBackticks = "`Hello, World!`"
    val nameEncoded = "Hello$u002C$u0020World$bang"
    val objectNameEncoded = "Hello$u002C$u0020World$bang$"
    assertAllNames(PackageId(parent, name), nameWithBackticks, nameEncoded,
      prefixScala + nameWithBackticks, prefixJvm + nameEncoded)
    assertAllNames(TypeId(parent, name), nameWithBackticks, nameEncoded,
      prefixScala + nameWithBackticks, prefixJvm + nameEncoded)
    assertAllNames(ObjectId(parent, name), nameWithBackticks, objectNameEncoded,
      prefixScala + nameWithBackticks, prefixJvm + objectNameEncoded)
  }

  test("Nested Java id types") {
    val id = PackageId("a", "b").getType("c").getType("d").getType("e")
    assertAllNames(id, "e", "e", "a.b.c.d.e", "a.b.c$d$e")
  }

  test("Nested Java id objects") {
    val id = PackageId("a", "b").getObject("c").getObject("d").getObject("e")
    assertAllNames(id, "e", "e$", "a.b.c.d.e", "a.b.c$d$e$")
  }

  test("Nested non-Java id types") {
    val id = PackageId("a", "b").getType("+").getType("+").getType("+")
    assertAllNames(id, "`+`", "$plus", "a.b.`+`.`+`.`+`", "a.b.$plus$$plus$$plus")
  }

  test("Nested non-Java id objects") {
    val id = PackageId("a", "b").getObject("+").getObject("+").getObject("+")
    assertAllNames(id, "`+`", "$plus$", "a.b.`+`.`+`.`+`", "a.b.$plus$$plus$$plus$")
  }
}
