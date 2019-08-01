package loamstream.model

import org.scalatest.FunSuite
import loamstream.loam.LoamScriptContext
import loamstream.TestHelpers
import loamstream.compiler.LoamPredef
import loamstream.loam.LoamCmdTool
import loamstream.util.BiMap

/**
 * @author clint
 * Nov 9, 2017
 */
final class ToolTest extends FunSuite {
  //TODO: More!!!
  
  test("tag") {
    implicit val scriptContext: LoamScriptContext = new LoamScriptContext(TestHelpers.emptyProjectContext)
    
    import loamstream.loam.LoamSyntax._
    
    def graph = scriptContext.projectContext.graph
    
    assert(graph.tools === Set.empty)
    assert(graph.namedTools === BiMap.empty)
    
    val tool0 = cmd"foo --bar --baz"
    val tool1 = cmd"bar --baz"
    
    assert(graph.tools === Set(tool0, tool1))
    assert(graph.nameOf(tool0).isDefined)
    assert(graph.nameOf(tool1).isDefined)
    
    val namedTool0 = tool0.tag("foo")
    
    assert(namedTool0 eq tool0)
    
    assert(graph.tools === Set(tool0, tool1))
    assert(graph.nameOf(tool0) === Some("foo"))
    assert(graph.nameOf(tool1).isDefined)
    
    val namedTool1 = tool1.tag("bar")
    
    assert(namedTool1 eq tool1)
    
    assert(graph.tools === Set(tool0, tool1))
    assert(graph.namedTools.toMap === Map(tool0 -> "foo", tool1 -> "bar"))
    
    val tool2 = cmd"baz"
    
    assert(graph.tools === Set(tool0, tool1, tool2))
    assert(graph.nameOf(tool0) === Some("foo"))
    assert(graph.nameOf(tool1) === Some("bar"))
    
    //non-unique name
    intercept[Exception] {
      tool2.tag("foo")
    }
    
    //re-naming
    intercept[Exception] {
      tool0.tag("sadf")
    }
  }
}
