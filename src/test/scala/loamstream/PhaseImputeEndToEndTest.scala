package loamstream

import loamstream.model.HasAst
import loamstream.tools.core.CoreTool
import java.nio.file.Path
import java.io.File
import loamstream.model.{AST, Tool}
import org.scalatest.FunSuite
import loamstream.conf.ImputationConfig
import loamstream.model.jobs.LToolBox
import loamstream.tools.core.CoreToolBox
import java.nio.file.Paths
import loamstream.model.execute.ChunkedExecuter
import loamstream.util.Hit
import loamstream.model.execute.LExecuter

/**
 * @author clint
 * date: Jun 13, 2016
 */
final class PhaseImputeEndToEndTest extends FunSuite with LoamTestHelpers {
  //NB: Ignored, since this relies on external tools
  ignore("Run shapeit, then impute2 on example data") {
    import scala.concurrent.ExecutionContext.Implicits.global
    
    val (_, executable) = toExecutable(compileFile("src/test/resources/loam/impute.loam"))

    val results = run(executable)

    assert(results.size == 2)

    //Basically, see that each pipeline step finished with a non-zero status
    assert(results.values.forall {
      case Hit(r) => r.isSuccess
      case _ => false
    })

    //TODO: More; Ideally, we want to know we're computing the expected results 
  }
}
