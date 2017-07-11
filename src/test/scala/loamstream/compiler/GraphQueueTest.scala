package loamstream.compiler

import org.scalatest.FunSuite
import loamstream.loam.LoamGraph
import loamstream.loam.LoamProjectContext
import loamstream.TestHelpers
import loamstream.util.ValueBox
import loamstream.loam.LoamCmdTool
import loamstream.loam.LoamScriptContext
import org.scalatest.Assertions

/**
 * @author clint
 * Jul 11, 2017
 */
final class GraphQueueTest extends FunSuite {
  
  import GraphQueueTest.makeTestGraphs
  
  test("enqueue/dequeue/isEmpty/nonEmpty") {
    val queue = new GraphQueue
    
    val (g0, g1, g2) = makeTestGraphs()
    
    assert(queue.isEmpty === true)
    assert(queue.nonEmpty === false)
    
    intercept[Exception] {
      queue.dequeue()
    }
    
    queue.enqueue(() => g0)
    
    assert(queue.isEmpty === false)
    assert(queue.nonEmpty === true)
    
    queue.enqueue(() => g1)
    
    assert(queue.isEmpty === false)
    assert(queue.nonEmpty === true)
    
    queue.enqueue(() => g2)
    
    assert(queue.isEmpty === false)
    assert(queue.nonEmpty === true)
    
    val actual0 = queue.dequeue()
    
    assert(queue.isEmpty === false)
    assert(queue.nonEmpty === true)
    
    val actual1 = queue.dequeue()
    
    assert(queue.isEmpty === false)
    assert(queue.nonEmpty === true)
    
    val actual2 = queue.dequeue()
    
    assert(queue.isEmpty === true)
    assert(queue.nonEmpty === false)
    
    assert(actual0() === g0)
    assert(actual1() === g1)
    assert(actual2() === g2)
  }
  
  test("orElseJust/GraphQueue.apply") {
    val (g0, g1, g2) = makeTestGraphs()
    
    //Empty case 
    {
      val emptyQueue = GraphQueue.empty
      
      assert(emptyQueue.isEmpty === true)
      
      val q = emptyQueue.orElseJust(g0)
      
      assert(emptyQueue.isEmpty === true)
      assert(q.isEmpty === false)
      assert(q.dequeue().apply() === g0)
      assert(q.isEmpty === true)
    }
    //Non-empty case
    {
      val queue = GraphQueue(() => g0, () => g1)
      
      assert(queue.isEmpty === false)
      
      val q = queue.orElseJust(g2)
      
      assert(q.isEmpty === false)
      
      assert(q.dequeue().apply() === g0)
      assert(q.dequeue().apply() === g1)
      
      assert(q.isEmpty === true)
    }
  }
  
  test("empty") {
    val queue = GraphQueue.empty
    
    assert(queue.isEmpty === true)
    
    val (g0, g1, g2) = makeTestGraphs()
    
    queue.enqueue(() => g2)
    
    assert(queue.isEmpty === false)
    
    assert(queue.dequeue().apply() === g2)
  }
}

object GraphQueueTest extends Assertions {
  private[compiler] def makeTestGraphs(): (LoamGraph, LoamGraph, LoamGraph) = {
    val g0 = LoamGraph.empty
    
    val projectCtx = new LoamProjectContext(TestHelpers.config, ValueBox(g0), GraphQueue.empty)
    
    import LoamCmdTool._
    
    assert(projectCtx.graph eq g0)

    implicit val scriptCtx = new LoamScriptContext(projectCtx)
    
    val t0 = cmd"foo"
    
    val g1 = projectCtx.graph
    
    val t1 = cmd"bar"
    
    val g2 = projectCtx.graph
    
    assert(g0 !== g1)
    assert(g1 !== g2)
    assert(g0 !== g2)
    
    (g0, g1, g2)
  }
}
