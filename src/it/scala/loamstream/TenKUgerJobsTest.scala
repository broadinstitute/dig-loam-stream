package loamstream

import org.scalatest.FunSuite
import loamstream.util.Files
import IntegrationTestHelpers.path
import java.nio.file.Files.exists
import java.nio.file.Path
import loamstream.util.Paths

/**
 * @author clint
 * Aug 7, 2020
 */
final class TenKUgerJobsTest extends FunSuite {
  /**
   * Run 10k Uger jobs, to verify that they can be run (and their statuses polled, etc)
   * in a perfomant manner.  This was useful after the switch from DRMAA to the q* tools.
   */
  test("10k jobs") {
    val N = 1000

    val M = 10

    def loamCode(workDir: Path, fileToCopy: Path) = s"""
object tenK extends loamstream.LoamFile {
  val in = store("${fileToCopy}").asInput
  
  drm {
    (1 to $N).foreach { i =>
      val out = store(s"${workDir}/outs/$${i}/$${i}.out")
  
      cmd"cp $$in $$out".in(in).out(out)
  
      (1 to $M).foreach { j =>
        val out2 = store(s"${workDir}/outs/$${i}/$${i}-$${j}.out")
  
        cmd"cp $$out $$out2".in(out).out(out2)
      }
    }
  }
}
"""
    IntegrationTestHelpers.withWorkDirUnderTarget(deleteWhenDone = false) { workDir =>
      val loamFile = workDir.resolve("tenK.scala")
      
      workDir.toFile.mkdirs()
      
      val fileToCopy = path("src/test/resources/a.txt")
      
      assert(exists(fileToCopy))
      
      Files.writeTo(loamFile)(loamCode(workDir, fileToCopy))
      
      val outputDir = workDir.resolve("outs")
      
      import Paths.Implicits.PathHelpers

      val numberedOutputDirs = (1 to N).map(i => outputDir.resolve(i.toString))
      
      for {
        numberedOutputDir <- numberedOutputDirs
      } {
        assert(numberedOutputDir.toFile.mkdirs(), s"Couldn't create '${numberedOutputDir}'")
      }
      
      Thread.sleep(30 * 1000)
      
      loamstream.apps.Main.main(Array("--backend", "uger", "--loams", loamFile.toString))
      
      for {
        (numberedOutputDir, i) <- numberedOutputDirs.zip(1 to N)
        j <- 1 to M
      } {
        assert(
            exists(numberedOutputDir.resolve(s"${i}-${j}.out")), 
            s"Couldn't find '${numberedOutputDir.resolve(s"${i}-${j}.out")}'")
      }
    }
  }
}
