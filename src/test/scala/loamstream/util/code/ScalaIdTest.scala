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

  test("Root package") {
    assertAllNames(RootPackageId, "_root_", "_root_", "_root_", "_root_")
  }

  test("Top level Java id") {
    assertAllNames(PackageId("test"), "test", "test", "test", "test")
    assertAllNames(TypeId("Test"), "Test", "Test", "Test", "Test")
    assertAllNames(ObjectId("Test"), "Test", "Test$", "Test", "Test$")
  }

  test("Top level non-Java id.") {
    val name = "Hello, World!"
    val nameWithBackticks = "`Hello, World!`"
    val nameEncoded = "Hello$u002C$u0020World$bang"
    val objectNameEncoded = "Hello$u002C$u0020World$bang$"
    assertAllNames(PackageId(name), nameWithBackticks, nameEncoded, nameWithBackticks, nameEncoded)
    assertAllNames(TypeId(name), nameWithBackticks, nameEncoded, nameWithBackticks, nameEncoded)
    assertAllNames(ObjectId(name), nameWithBackticks, objectNameEncoded, nameWithBackticks, objectNameEncoded)
  }

  test("Java id in package") {
    val parent = PackageId("parent")
  }

}
