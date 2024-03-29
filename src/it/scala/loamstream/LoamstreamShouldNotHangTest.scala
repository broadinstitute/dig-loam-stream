package loamstream

import org.scalatest.FunSuite
import java.nio.file.Path
import loamstream.util.Files
import loamstream.util.Paths

/**
 * @author clint
 * Sep 20, 2017
 */
final class LoamstreamShouldNotHangTest extends FunSuite {
  import IntegrationTestHelpers.path
  import java.nio.file.Files.exists

  private val nonexistentPath = path("/foo/bar/baz/blerg/zerg/glerg")
  
  private val className = getClass.getSimpleName
  
  private val outDir = path(s"target/$className")
  
   /*
    * As stores:
    *                          +--> storeY 
    *                         /
    *   nonexistent --> storeX
    *                         \
    *                          +--> storeZ
    *           
    * As jobs:
    *            +--> shouldWork0
    *           /     
    *   willFail
    *           \
    *            +--> shouldWork1
    */
  test("Loamstream should not hang - minimal version") {

    import Paths.Implicits.PathHelpers
    
    Files.createDirsIfNecessary(outDir)
    
    val xPath = outDir / "minimal-x.txt"
    val yPath = outDir / "minimal-y.txt"
    val zPath = outDir / "minimal-z.txt"
    
    assert(!exists(nonexistentPath))
    assert(!exists(xPath))
    assert(!exists(yPath))
    assert(!exists(zPath))
    
    val loamCode = s"""|
                       |val nonexistent = store("$nonexistentPath").asInput
                       |val storeX = store("$xPath")
                       |val storeY = store("$yPath")
                       |val storeZ = store("$zPath")
                       |
                       |drm {
                       |  //Will fail
                       |  cmd"cp $$nonexistent $$storeX".in(nonexistent).out(storeX)
                       |
                       |  //Would work if previous cmd worked
                       |  cmd"cp $$storeX $$storeY".in(storeX).out(storeY)
                       |  cmd"cp $$storeX $$storeZ".in(storeX).out(storeZ)
                       |}
                       |""".stripMargin
    
    run(s"$className-minimal", loamCode)
    
    assert(!exists(nonexistentPath))
    assert(!exists(xPath))
    assert(!exists(yPath))
    assert(!exists(zPath))
  }
  
  /*
   * As stores:
   *   storeA --> storeB -->  storeC --> storeD
   *                 \
   *                  +--> storeX --> storeY
   *                 /
   *            nonexistent 
   *            
   * As jobs:
   *   
   *   a2b --> b2c --> c2d
   *      \
   *       +- willFail --> x2y           
   */
  test("Loamstream should not hang - more-complex version") {
    
    import Paths.Implicits.PathHelpers
    
    Files.createDirsIfNecessary(outDir)
    
    val aPath = path("src/test/resources/a.txt")
    val bPath = outDir / "b.txt"
    val cPath = outDir / "c.txt"
    val dPath = outDir / "d.txt"
    val xPath = outDir / "x.txt"
    val yPath = outDir / "y.txt"
    
    assert(!exists(nonexistentPath))
    assert(exists(aPath))
    assert(!exists(bPath))
    assert(!exists(cPath))
    assert(!exists(dPath))
    assert(!exists(xPath))
    assert(!exists(yPath))
    
    val loamCode = s"""|
                       |val nonexistent = store("$nonexistentPath").asInput
                       |val storeX = store(s"$xPath")
                       |val storeY = store(s"$yPath")
                       |
                       |val storeA = store("$aPath").asInput
                       |val storeB = store(s"$bPath")
                       |val storeC = store(s"$cPath")
                       |val storeD = store(s"$dPath")
                       |
                       |drm {
                       |  //Should work
                       |  cmd"cp $$storeA $$storeB".in(storeA).out(storeB)
                       |  cmd"cp $$storeB $$storeC".in(storeB).out(storeC)
                       |  cmd"cp $$storeC $$storeD".in(storeC).out(storeD)
                       |  //Will fail
                       |  cmd"cp $$nonexistent $$storeX && cp $$storeB $$storeX".in(nonexistent, storeB).out(storeX)
                       |  //Shouldn't run
                       |  cmd"cp $$storeX $$storeY".in(storeX).out(storeY)
                       |}
                       |""".stripMargin
    
    run(s"$className-complex", loamCode)
    
    assert(!exists(nonexistentPath))
    assert(exists(aPath))
    assert(exists(bPath))
    assert(exists(cPath))
    assert(exists(dPath))
    assert(!exists(xPath))
    assert(!exists(yPath))
    
    //Assert that commands we expect to have worked actually copied things
    assertSameContents(aPath, bPath)
    assertSameContents(aPath, cPath)
    assertSameContents(aPath, dPath)
  }
  
  private def assertSameContents(p0: Path, p1: Path): Unit = {
    import loamstream.util.Files.readFrom
    
    assert(readFrom(p0) === readFrom(p1))
  }
  
  private def run(baseFileName: String, loamCode: String): Unit = {
    import Paths.Implicits.PathHelpers
    import java.nio.file.Files.exists
    
    Files.createDirsIfNecessary(outDir)                  
    
    val loamFile = outDir / s"$baseFileName.loam"
    
    Files.writeTo(loamFile)(loamCode)
    
    //Recent Broad-FS issues have me gunshy
    assert(exists(loamFile))
    
    val confFile = outDir / s"$baseFileName.conf"
    
    Files.writeTo(confFile)(confFileContents)
    
    //Recent Broad-FS issues have me gunshy
    assert(exists(confFile))
    
    val ugerWorkDir = path("./uger")
    
    Files.createDirsIfNecessary(ugerWorkDir)
    
    //Recent Broad-FS issues have me gunshy
    assert(exists(ugerWorkDir))

    val args: Array[String] = {
      Array(
          "--backend",
          "uger",
          "--conf",
          confFile.toString,
          "--loams",
          loamFile.toString)
    }
    
    loamstream.apps.Main.main(args)
  }
  
  private val confFileContents: String = {
    //NB: Don't configure Google support, allow only 2 restarts, and ask Uger for 1g of ram.
    //This makes for less-noisy logs and less queueing.
    s"""|loamstream {
        |  execution {
        |    maxRunsPerJob = 3
        |  }
        |  
        |  uger {
        |    maxNumJobs = 2400
        |    workDir = "uger"
        |  }
        |}
        |""".stripMargin.trim
  }
}
