package loamstream.util

import org.scalatest.FunSuite

/**
 * @author clint
 * Nov 13, 2019
 */
final class HeterogeneousMapTest extends FunSuite {
  import HeterogeneousMap.keyFor
  
  private val k0 = keyFor[String].of(123)
  private val k1 = keyFor[String].of(456)
  
  private val v0 = "foo"
  private val v1 = "bar"
  
  test("empty") {
    assert(HeterogeneousMap.empty.isEmpty === true)
    assert(HeterogeneousMap.empty.size === 0)
  }
  
  test("apply - no params") {
    assert(HeterogeneousMap() === HeterogeneousMap.empty)
  }
  
  test("+ / apply / get / isEmpty / size") {
    import HeterogeneousMap._
    
    val m0 = HeterogeneousMap.empty
    
    assert(m0.isEmpty)
    
    val m1 = m0 + k0 ~> v0
    
    assert((m1 eq m0) === false)
    
    assert(m0.isEmpty)
    assert(m1.isEmpty === false)
    assert(m1.size === 1)
    assert(m1(k0) === v0)
    assert(m1.get(k0) === Some(v0))
    
    val m2 = m1 + k1 ~> v1
    
    assert((m2 eq m1) === false)
    
    assert(m0.isEmpty)
    assert(m1.size === 1)
    assert(m1(k0) === v0)
    assert(m1.get(k0) === Some(v0))
    
    assert(m2.isEmpty === false)
    assert(m2.size === 2)
    assert(m2(k0) === v0)
    assert(m2(k1) === v1)
    assert(m2.get(k0) === Some(v0))
    assert(m2.get(k1) === Some(v1))
    
    assert(m0.get(k0) === None)
    assert(m0.get(k1) === None)
    assert(m1.get(k1) === None)
  }
  
  test("++") {
    import HeterogeneousMap._
    
    assert(empty ++ Nil === empty)
    
    val m1 = empty ++ Seq(k0 ~> v0)
    
    assert(m1.size === 1)
    assert(m1(k0) === v0)
    
    val m2 = empty ++ Seq(k0 ~> v0, k1 ~> v1)
    
    assert(m2.size === 2)
    assert(m2(k0) === v0)
    assert(m2(k1) === v1)
  }
  
  test("contains") {
    import HeterogeneousMap._
    
    assert(empty.contains(k0) === false)
    assert(empty.contains(k1) === false)
    
    val m1 = empty ++ Seq(k0 ~> v0)
    
    assert(m1.contains(k0) === true)
    assert(m1.contains(k1) === false)
    
    val m2 = empty ++ Seq(k0 ~> v0, k1 ~> v1)
    
    assert(m2.contains(k0) === true)
    assert(m2.contains(k1) === true)
  }
  
  test("foreach") {
    import HeterogeneousMap._
    
    {
      var n = 0
      
      empty.foreach(_ => n += 1)

      assert(n === 0)
    }
    
    def fold[A](m: HeterogeneousMap)(z: A)(f: (A, (Any, Any)) => A): A = {
      var result = z
      
      m.foreach { tuple =>
        result = f(result, tuple)
      }
      
      result
    }
    
    val m1 = empty ++ Seq(k0 ~> v0)
    
    val m2 = empty ++ Seq(k0 ~> v0, k1 ~> v1)

    val z: (Set[Int], Set[String]) = (Set.empty, Set.empty)
    
    {
      val (m1Keys, m1Values) = fold(m1)(z) { (acc, tuple) =>
        val (keys, values) = acc
        
        val (k: Int, v: String) = tuple
        
        (keys + k, values + v)
      }
      
      assert(m1Keys === Set(k0.key))
      assert(m1Values === Set(v0))
    }
    
    {
      val (m2Keys, m2Values) = fold(m2)(z) { (acc, tuple) =>
        val (keys, values) = acc
        
        val (k: Int, v: String) = tuple
        
        (keys + k, values + v)
      }
      
      assert(m2Keys === Set(k0.key, k1.key))
      assert(m2Values === Set(v0, v1))
    }
  }
  
  test("keyFor / ~>") {
    import HeterogeneousMap.keyFor
    import HeterogeneousMap.Key
    import HeterogeneousMap.Entry
    
    val key: Key[Int, String] = keyFor[String].of(123)
    
    val entry = key ~> "foo"
    
    assert(entry === Entry(key, "foo"))
  }
}
