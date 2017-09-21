package loamstream.integration

import org.scalatest.FunSuite
import java.nio.file.Paths
import java.nio.file.Path
import java.nio.file.Files
import loamstream.LoamTestHelpers
import loamstream.compiler.LoamProject
import loamstream.loam.LoamScript
import loamstream.util.Shot
import loamstream.TestHelpers
import loamstream.util.Loggable

/**
 * @author clint
 * Dec 21, 2016
 */
final class CompileLoamsTest extends FunSuite with LoamTestHelpers with Loggable {
  private val loamDir = Paths.get("pipeline/loam/")
  
  test(s"Compile all files in $loamDir") {
    val loams: Set[Path] = {
      import scala.collection.JavaConverters._
      
      Files.list(loamDir).iterator.asScala.filter(_.toString.endsWith(".loam")).toSet
    }
    
    info(s"Compiling ${loams.size} files:")
    loams.toSeq.sortBy(_.toString).foreach(loam => info(s"  $loam"))

    val scripts: Set[LoamScript] = Shot.sequence(loams.map(LoamScript.read)).get    

    val results = compile(LoamProject(TestHelpers.config, scripts))
    
    assert(results.errors === Nil)
  }
}
