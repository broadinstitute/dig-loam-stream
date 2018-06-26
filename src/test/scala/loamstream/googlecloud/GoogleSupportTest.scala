package loamstream.googlecloud

import java.nio.file.Paths
import org.scalatest.FunSuite
import loamstream.TestHelpers
import loamstream.loam.LoamScriptContext
import loamstream.compiler.LoamPredef
import loamstream.loam.LoamCmdTool
import loamstream.util.BashScript.Implicits._

/**
 * @author clint
 * Nov 2, 2017
 */
final class GoogleSupportTest extends FunSuite {
  private val gsutil = Paths.get("/humgen/diabetes/users/dig/loamstream/google-cloud-sdk/bin/gsutil")
  private val gsutilPath = gsutil.toAbsolutePath.render

  test("googleCopy - one pair, no params") {
    TestHelpers.withScriptContext { implicit context =>
      import GoogleSupport._
      import LoamPredef._

      val graphSoFar = context.projectContext.graph

      assert(graphSoFar.tools.isEmpty)
      assert(graphSoFar.stores.isEmpty)

      val local = store.at(path("/some/local/path"))
      val remote = store.at(uri("gs://loamstream/foo/bar/baz"))

      assert(graphSoFar.tools.isEmpty)

      googleCopy()(local, remote)

      //No inputs, so nothing should have been done
      
      val graphAtEnd = context.projectContext.graph

      assert(graphAtEnd.stores === Set(local, remote))

      assert(graphAtEnd.tools.size === 1)

      val onlyTool = graphAtEnd.tools.head.asInstanceOf[LoamCmdTool]

      val expectedCommandLine = s"$gsutilPath cp  /some/local/path gs://loamstream/foo/bar/baz"
      val actualCommandLine = onlyTool.commandLine

      assert(actualCommandLine === expectedCommandLine)
    }
  }

  test("googleCopy - one pair, some params") {
    TestHelpers.withScriptContext { implicit context =>
      import GoogleSupport._
      import LoamPredef._
  
      val graphSoFar = context.projectContext.graph
  
      assert(graphSoFar.tools.isEmpty)
      assert(graphSoFar.stores.isEmpty)
  
      val local = store.at(path("/some/local/path"))
      val remote = store.at(uri("gs://loamstream/foo/bar/baz"))
  
      assert(graphSoFar.tools.isEmpty)
  
      googleCopy()(local, remote, "blerg", "zerg")
  
      //No inputs, so nothing should have been done
  
      val graphAtEnd = context.projectContext.graph
  
      assert(graphAtEnd.stores === Set(local, remote))
  
      assert(graphAtEnd.tools.size === 1)
  
      val onlyTool = graphAtEnd.tools.head.asInstanceOf[LoamCmdTool]
  
      val expectedCommandLine = s"$gsutilPath cp blerg zerg /some/local/path gs://loamstream/foo/bar/baz"
  
      assert(onlyTool.commandLine === expectedCommandLine)
    }
  }
}
