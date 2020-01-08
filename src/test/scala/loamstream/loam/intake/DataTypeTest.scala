package loamstream.loam.intake

import org.scalatest.FunSuite

/**
 * @author clint
 * Jan 3, 2020
 */
final class DataTypeTest extends FunSuite {
  test("name/toString") {
    import DataType._
    
    assert(Float.name === "FLOAT")
    assert(Int.name === "INTEGER")
    assert(String.name === "STRING")
    
    assert(Float.toString === "FLOAT")
    assert(Int.toString === "INTEGER")
    assert(String.toString === "STRING")
  }
  
  test("fromTypeTag") {
    import DataType.fromTypeTag
    import scala.reflect.runtime.universe._
    
    assert(fromTypeTag(typeTag[Int]) === DataType.Int)
    
    assert(fromTypeTag(typeTag[String]) === DataType.String)
    assert(fromTypeTag(typeTag[Long]) === DataType.Int)
    assert(fromTypeTag(typeTag[Int]) === DataType.Int)
    assert(fromTypeTag(typeTag[Short]) === DataType.String)
    assert(fromTypeTag(typeTag[Byte]) === DataType.String)
    assert(fromTypeTag(typeTag[Double]) === DataType.Float)
    assert(fromTypeTag(typeTag[Float]) === DataType.Float)
    
    assert(fromTypeTag(typeTag[DataTypeTest.Foo]) === DataType.String)
  }
}

object DataTypeTest {
  private final class Foo
}
