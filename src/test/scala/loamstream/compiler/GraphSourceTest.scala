package loamstream.compiler

import org.scalatest.FunSuite

/**
 * @author clint
 * Jul 11, 2017
 */
final class GraphSourceTest extends FunSuite {
  test("Empty") {
    val source = GraphSource.Empty
    
    assert(source.iterator.hasNext === false)
    
    intercept[Exception] {
      source.iterator.next()
    }
  }
  
  import GraphQueueTest.makeTestGraphs
  
  test("fromQueue") {
    val (g0, g1, g2) = makeTestGraphs()
    
    val queue = GraphQueue(() => g0, () => g1)
    
    val thunks = GraphSource.fromQueue(queue).iterator
    
    assert(queue.isEmpty === false)
    assert(thunks.hasNext === true)
    
    val actual0 = thunks.next().apply()
    
    assert(actual0 === g0)
    assert(queue.isEmpty === false)
    assert(thunks.hasNext === true)
    
    val actual1 = thunks.next().apply()
    
    assert(actual1 === g1)
    assert(queue.isEmpty === true)
    assert(thunks.hasNext === false)
    
    queue.enqueue(() => g2)
    
    assert(queue.isEmpty === false)
    assert(thunks.hasNext === true)
    
    val actual2 = thunks.next().apply()
    
    assert(actual2 === g2)
    assert(queue.isEmpty === true)
    assert(thunks.hasNext === false)
  }
}
