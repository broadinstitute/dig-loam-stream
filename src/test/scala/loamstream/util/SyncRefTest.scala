package loamstream.util

import org.scalatest.FunSuite

/**
 * @author clint
 * date: Aug 4, 2016
 */
final class SyncRefTest extends FunSuite {
  test("Companion object apply") {
    val v: SyncRef[Int] = SyncRef()
    
    assert(v.value.isEmpty)
  }
  
  test("Companion object apply with initial value") {
    val v: SyncRef[Int] = SyncRef(42)
    
    assert(v.value == Some(42))
    assert(v() == 42)
  }
  
  test("value_=") {
    val v: SyncRef[Int] = SyncRef(42)
    
    assert(v() == 42)
    
    v.value = Some(99)
    
    assert(v() == 99)
    
    v.value = None
    
    assert(v.value.isEmpty)
  }
  
  test("apply") {
    val v: SyncRef[Int] = SyncRef()
    
    intercept[Exception] {
      v()
    }
    
    v.value = Some(99)
    
    assert(v() == 99)
  }
  
  test("update") {
    val v: SyncRef[Int] = SyncRef(42)
    
    assert(v() == 42)
    
    v() = 99
    
    assert(v() == 99)
  }
  
  test("mutate") {
    val v: SyncRef[Int] = SyncRef(42)
    
    assert(v() == 42)
    
    v.mutate(_ + 1)
    
    assert(v() == 43)
  }
}