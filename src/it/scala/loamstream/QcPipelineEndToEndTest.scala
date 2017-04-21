package loamstream

import org.scalatest.FunSuite
import java.nio.file.Path
import loamstream.util.ExitCodes
import java.nio.file.Paths
import java.nio.file.Files

/**
 * @author clint
 * Apr 21, 2017
 */
final class QcPipelineEndToEndTest extends FunSuite {
  import Files.exists
  
  test("Run the QC pipeline end-to-end on real data") {
    run()

    val filesToCheck: Seq[Path] = Seq(path(""), path(""))
    
    val referenceDir = path("/humgen/diabetes/users/dig/loamstream/ci/test-data/qc/camp/results")
    val outputDir = path("./qc")
    
    val pairsToCompare: Seq[(Path, Path)] = filesToCheck.map(p => (referenceDir.resolve(p), outputDir.resolve(p)))
    
    pairsToCompare.foreach(diff.tupled)
  }
  
  private def path(s: String): Path = Paths.get(s)
  
  private def run(): Unit = {
    Files.createDirectory(path("./qc"))
    Files.createDirectory(path("./uger-scripts"))
    
    val args: Array[String] = Array()
    
    loamstream.apps.Main.main(args)
  }
  
  private val diff: (Path, Path) => Unit = { (a, b) =>
    import scala.sys.process._
    
    //NB: Shell out to diff
    val exitCode = s"diff -q $a $b".!
    
    assert(ExitCodes.isSuccess(exitCode), s"$a and $b differ, or $b doesn't exist")
  }
}
