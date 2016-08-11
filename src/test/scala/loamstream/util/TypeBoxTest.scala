package loamstream.util

import org.scalatest.FunSuite

/**
 * @author clint
 * date: Aug 9, 2016
 */
final class TypeBoxTest extends FunSuite {
  test("isSubTypeOf and isSuperTypeOf") {
    val sub = TypeBox.of[java.lang.Integer]

    val parent = TypeBox.of[java.lang.Number]

    val cousin = TypeBox.of[String]

    def doTest(
        isSub: (TypeBox[_], TypeBox[_]) => Boolean,
        isSuper: (TypeBox[_], TypeBox[_]) => Boolean): Unit = {

      assert(isSub(sub, sub))
      assert(isSub(parent, parent))
      assert(isSub(cousin, cousin))

      assert(isSuper(sub, sub))
      assert(isSuper(parent, parent))
      assert(isSuper(cousin, cousin))

      assert(isSub(sub, parent))

      assert(isSuper(parent, sub))

      assert(isSuper(parent, cousin) === false)
      assert(isSuper(sub, cousin) === false)
      assert(isSuper(cousin, parent) === false)
      assert(isSuper(cousin, sub) === false)

      assert(isSub(parent, cousin) === false)
      assert(isSub(sub, cousin) === false)
      assert(isSub(cousin, parent) === false)
      assert(isSub(cousin, sub) === false)
    }

    doTest(_ <:< _, _ >:> _)
    
    doTest(_.isSubTypeOf(_), _.isSuperTypeOf(_))
  }
}