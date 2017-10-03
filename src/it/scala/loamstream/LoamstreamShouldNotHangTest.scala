package loamstream

import org.scalatest.FunSuite
import java.nio.file.Path
import loamstream.util.Files
import loamstream.util.PathEnrichments

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

    import PathEnrichments.PathHelpers
    
    Files.createDirsIfNecessary(outDir)
    
    val xPath = outDir / "minimal-x.txt"
    val yPath = outDir / "minimal-y.txt"
    val zPath = outDir / "minimal-z.txt"
    
    assert(!exists(nonexistentPath))
    assert(!exists(xPath))
    assert(!exists(yPath))
    assert(!exists(zPath))
    
    val loamCode = s"""|
                       |val nonexistent = store[TXT].at("$nonexistentPath").asInput
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
    
    import PathEnrichments.PathHelpers
    
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
                       |val nonexistent = store[TXT].at("$nonexistentPath").asInput
                       |val storeX = store[TXT].at(s"$xPath")
                       |val storeY = store[TXT].at(s"$yPath")
                       |
                       |val storeA = store[TXT].at("$aPath").asInput
                       |val storeB = store[TXT].at(s"$bPath")
                       |val storeC = store[TXT].at(s"$cPath")
                       |val storeD = store[TXT].at(s"$dPath")
                       |
                       |uger {
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
    import PathEnrichments.PathHelpers
    
    Files.createDirsIfNecessary(outDir)                  
    
    val loamFile = outDir / s"$baseFileName.loam"
    
    Files.writeTo(loamFile)(loamCode)
    
    val confFile = outDir / s"$baseFileName.conf"
    
    Files.writeTo(confFile)(confFileContents)
    
    Files.createDirsIfNecessary(path("./uger"))

    val args: Array[String] = {
      Array(
          "--conf",
          confFile.toString,
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
        |    logFile = "uger.log"
        |    maxNumJobs = 2400
        |    workDir = "uger"
        |    nativeSpecification = "-clear -cwd -shell y -b n -l h_vmem=1g"
        |  }
        |}
        |""".stripMargin.trim
  }
}
