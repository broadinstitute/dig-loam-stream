package loamstream.util

import scala.reflect.runtime.universe._
import org.scalatest.FunSuite


/**
  * LoamStream
  * Created by oliverr on 1/20/2016.
  */
final class ProductTypeExploderTest extends FunSuite {

  private class A[T]

  private object A {

    class B

  }

  private case class C(i: Int, s: String, ai: A[Int], ab: A.B, d: Double)
  
  private def typeOf[A : TypeTag]: Type = typeTag[A].tpe
  
  import ProductTypeExploder.explode
  
  test("Should be able to explode Tuple containing user-defined types") {
    val complex = typeOf[(Int, String, A[Int], A.B, Double)]
    
    val exploded = explode(complex)
    
    assert(exploded === Seq(typeOf[Int], typeOf[String], typeOf[A[Int]], typeOf[A.B], typeOf[Double]))
  }
  
  test("Should be able to explode 1-Tuples") {
    def doTest[A : TypeTag](): Unit = {
      val tupleType = typeOf[Tuple1[A]]
    
      val exploded = explode(tupleType)
    
      assert(exploded === Seq(typeOf[A]))
    }
    
    doTest[Int]()
    doTest[String]()
    doTest[C]()
    doTest[A[Double]]()
    doTest[A.B]()
  }
  
  test("Should be able to explode non-Tuple type to itself") {
    def doTest[A : TypeTag](): Unit = {
      assert(explode(typeOf[A]) === Seq(typeOf[A]))
    }
    
    doTest[String]()
    doTest[A[Int]]()
    doTest[A.B]()
    doTest[C]()
  }
}
