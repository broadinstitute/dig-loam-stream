package loamstream

import org.scalatest.FunSuite

import loamstream.util.Hit

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
    assert(results.values.forall(_.isSuccess))

    //TODO: More; Ideally, we want to know we're computing the expected results 
  }
}
