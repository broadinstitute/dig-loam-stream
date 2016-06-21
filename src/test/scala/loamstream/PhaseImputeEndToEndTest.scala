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
final class PhaseImputeEndToEndTest extends FunSuite {
  import PhaseImputeEndToEndTest._

  //NB: Ignored, since this relies on external tools
  ignore("Run shapeit, then impute2 on example data") {
    val inputFile = Paths.get("src/test/resources/imputation/gwas.vcf.gz")

    val output = Paths.get("target/bar")

    val (toolbox, ast, executer) = doSetup(inputFile, output)

    val executable = toolbox.createExecutable(ast)

    val results = executer.execute(executable)

    assert(results.size == 2)

    //Basically, see that each pipeline step finished with a non-zero status
    assert(results.values.forall {
      case Hit(r) => r.isSuccess
      case _ => false
    })

    //TODO: More; Ideally, we want to know we're computing the expected results 
  }

  private def doSetup(inputFile: Path, output: Path): (LToolBox, AST, LExecuter) = {

    val config = ImputationConfig.fromFile("src/test/resources/loamstream-test.conf").get

    val ast = shapeItImpute2PipelineAst(config, inputFile.toAbsolutePath, output.toAbsolutePath)

    val toolbox = CoreToolBox(LEnv.empty)
    
    val executer = ChunkedExecuter.default
    
    (toolbox, ast, executer)
  }
}

object PhaseImputeEndToEndTest {

  def shapeItImpute2PipelineAst(config: ImputationConfig, inputVcf: Path, outputFile: Path): AST = {
    val tempFile = File.createTempFile("shapeit", "loamstream").toPath.toAbsolutePath

    val shapeItTool = CoreTool.Phase(config.shapeIt, inputVcf, tempFile)

    val impute2Tool = CoreTool.Impute(config.impute2, shapeItTool.outputHaps, outputFile)

    import Tool.ParamNames.{ input, output }

    AST(impute2Tool).get(input).from(AST(shapeItTool)(output))
  }
}