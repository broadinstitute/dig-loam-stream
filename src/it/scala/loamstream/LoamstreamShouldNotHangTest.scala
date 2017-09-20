package loamstream

import org.scalatest.FunSuite
import java.nio.file.Path

/**
 * @author clint
 * Sep 20, 2017
 */
final class LoamstreamShouldNotHangTest extends FunSuite {
  import IntegrationTestHelpers.path
  import java.nio.file.Files.exists

  test("Loamstream should not hang - minimal version") {
    
    val nonexistenPath = path("/foo/bar/baz/blerg/zerg/glerg")

    val className = getClass.getSimpleName
    val xPath = path(s"target/$className-x.txt")
    val yPath = path(s"target/$className-y.txt")
    val zPath = path(s"target/$className-z.txt")
    
    assert(!exists(nonexistenPath))
    assert(!exists(xPath))
    assert(!exists(yPath))
    assert(!exists(zPath))
    
    val loamCode = s"""|
                       |val nonexistent = store[TXT].at("$nonexistenPath")
                       |val storeX = store[TXT].at("$xPath")
                       |val storeY = store[TXT].at("$yPath")
                       |val storeZ = store[TXT].at("$zPath")
                       |
                       |uger {
                       |  //Will fail
                       |  cmd"cp $$nonexistent $$storeX".in(nonexistent).out(storeX)
                       |
                       |  //Would work if previous cmd worked
                       |  cmd"cp $$storeX $$storeY".in(storeX).out(storeY)
                       |  cmd"cp $$storeX $$storeZ".in(storeX).out(storeZ)
                       |}
                       |""".stripMargin
    
    import loamstream.util.Files
                      
    val loamFile = path("target/$className-minimal.loam")
    
    Files.writeTo(loamFile)(loamCode)
    
    Files.createDirsIfNecessary(path("./uger"))

    val args: Array[String] = {
      Array(
          "--conf",
          "pipeline/conf/loamstream.conf",
          loamFile.toString)
    }
    
    loamstream.apps.Main.main(args)
    
    assert(!exists(nonexistenPath))
    assert(!exists(xPath))
    assert(!exists(yPath))
    assert(!exists(zPath))
  }
}
