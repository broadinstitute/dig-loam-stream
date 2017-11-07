package loamstream.googlecloud

import org.scalatest.FunSuite
import loamstream.TestHelpers
import loamstream.loam.LoamScriptContext
import loamstream.compiler.LoamPredef
import loamstream.loam.LoamCmdTool

/**
 * @author clint
 * Nov 2, 2017
 */
final class GoogleSupportTest extends FunSuite {
  private val gsutilPath = "/humgen/diabetes/users/dig/loamstream/google-cloud-sdk/bin/gsutil"
  
  test("googleCopy - one pair, no params") {
    implicit val context = new LoamScriptContext(TestHelpers.emptyProjectContext)
    
    import GoogleSupport._
    import LoamPredef._
      
    val graphSoFar = context.projectContext.graph
      
    assert(graphSoFar.tools.isEmpty)
    assert(graphSoFar.stores.isEmpty)
    
    val local = store.at(path("/some/local/path"))
    val remote = store.at(uri("gs://loamstream/foo/bar/baz"))
    
    assert(graphSoFar.tools.isEmpty)
    
    googleCopy(local, remote)
    
    //No inputs, so nothing should have been done
    
    val graphAtEnd = context.projectContext.graph
    
    assert(graphAtEnd.stores === Set(local, remote))
    
    assert(graphAtEnd.tools.size === 1)
    
    val onlyTool = graphAtEnd.tools.head.asInstanceOf[LoamCmdTool]
    
    val expectedCommandLine = s"$gsutilPath cp  /some/local/path gs://loamstream/foo/bar/baz"
    
    assert(onlyTool.commandLine === expectedCommandLine)
  }
  
  test("googleCopy - one pair, some params") {
    implicit val context = new LoamScriptContext(TestHelpers.emptyProjectContext)
    
    import GoogleSupport._
    import LoamPredef._
      
    val graphSoFar = context.projectContext.graph
      
    assert(graphSoFar.tools.isEmpty)
    assert(graphSoFar.stores.isEmpty)
    
    val local = store.at(path("/some/local/path"))
    val remote = store.at(uri("gs://loamstream/foo/bar/baz"))
    
    assert(graphSoFar.tools.isEmpty)
    
    googleCopy(local, remote, "blerg", "zerg")
    
    //No inputs, so nothing should have been done
    
    val graphAtEnd = context.projectContext.graph
    
    assert(graphAtEnd.stores === Set(local, remote))
    
    assert(graphAtEnd.tools.size === 1)
    
    val onlyTool = graphAtEnd.tools.head.asInstanceOf[LoamCmdTool]
    
    val expectedCommandLine = s"$gsutilPath cp blerg zerg /some/local/path gs://loamstream/foo/bar/baz"
    
    assert(onlyTool.commandLine === expectedCommandLine)
  }
  
  test("googleCopy - no pairs") {
    val graph = TestHelpers.makeGraph { implicit context =>
      import GoogleSupport._
      
      val graphSoFar = context.projectContext.graph
      
      assert(graphSoFar.tools.isEmpty)
      assert(graphSoFar.stores.isEmpty)
      
      googleCopy(Nil, Nil)
    }
    
    //No inputs, so nothing should have been done
    
    assert(graph.tools.isEmpty)
    assert(graph.stores.isEmpty)
  }
  
  test("googleCopy - many pairs, no params") {
    implicit val context = new LoamScriptContext(TestHelpers.emptyProjectContext)
    
    import GoogleSupport._
    import LoamPredef._
      
    val graphSoFar = context.projectContext.graph
      
    assert(graphSoFar.tools.isEmpty)
    assert(graphSoFar.stores.isEmpty)
    
    val local0 = store.at(path("/some/local/path"))
    val remote0 = store.at(uri("gs://loamstream/foo/bar/baz"))
    
    val local1 = store.at(path("/some/other/path"))
    val remote1 = store.at(uri("gs://loamstream/blah/blah/blerg"))
    
    assert(graphSoFar.tools.isEmpty)
    
    googleCopy(Seq(local0, local1), Seq(remote0, remote1))
    
    //No inputs, so nothing should have been done
    
    val graphAtEnd = context.projectContext.graph
    
    assert(graphAtEnd.stores === Set(local0, local1, remote0, remote1))
    
    assert(graphAtEnd.tools.size === 2)
    
    val tools = graphAtEnd.tools.map(_.asInstanceOf[LoamCmdTool])
    
    val expectedCommandLines = Set(
        s"$gsutilPath cp  /some/local/path gs://loamstream/foo/bar/baz",
        s"$gsutilPath cp  /some/other/path gs://loamstream/blah/blah/blerg")
    
    assert(tools.map(_.commandLine) === expectedCommandLines)
  }
  
  test("googleCopy - many pairs, some params") {
    implicit val context = new LoamScriptContext(TestHelpers.emptyProjectContext)
    
    import GoogleSupport._
    import LoamPredef._
      
    val graphSoFar = context.projectContext.graph
      
    assert(graphSoFar.tools.isEmpty)
    assert(graphSoFar.stores.isEmpty)
    
    val local0 = store.at(path("/some/local/path"))
    val remote0 = store.at(uri("gs://loamstream/foo/bar/baz"))
    
    val local1 = store.at(path("/some/other/path"))
    val remote1 = store.at(uri("gs://loamstream/blah/blah/blerg"))
    
    assert(graphSoFar.tools.isEmpty)
    
    googleCopy(Seq(local0, local1), Seq(remote0, remote1), "nuh", "zuh")
    
    //No inputs, so nothing should have been done
    
    val graphAtEnd = context.projectContext.graph
    
    assert(graphAtEnd.stores === Set(local0, local1, remote0, remote1))
    
    assert(graphAtEnd.tools.size === 2)
    
    val tools = graphAtEnd.tools.map(_.asInstanceOf[LoamCmdTool])
    
    val expectedCommandLines = Set(
        s"$gsutilPath cp nuh zuh /some/local/path gs://loamstream/foo/bar/baz",
        s"$gsutilPath cp nuh zuh /some/other/path gs://loamstream/blah/blah/blerg")
    
    assert(tools.map(_.commandLine) === expectedCommandLines)
  }
}
