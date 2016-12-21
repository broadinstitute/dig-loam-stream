package loamstream.integration

import org.scalatest.FunSuite
import java.nio.file.Paths
import java.nio.file.Path
import java.nio.file.Files
import loamstream.LoamTestHelpers
import loamstream.compiler.LoamProject
import loamstream.loam.LoamScript
import loamstream.util.Shot

/**
 * @author clint
 * Dec 21, 2016
 */
final class CompileQcStepsTest extends FunSuite with LoamTestHelpers {
  private val qcDir = Paths.get("src/main/loam/qc/")
  
  test(s"Compile all files in $qcDir") {
    val loams: Set[Path] = {
      import scala.collection.JavaConverters._
      
      Files.list(qcDir).iterator.asScala.filter(_.toString.endsWith(".loam")).toSet
    }

    val scripts: Set[LoamScript] = Shot.sequence(loams.map(LoamScript.read)).get    

    val results = compile(LoamProject(scripts))
    
    assert(results.errors === Nil)
  }
}
