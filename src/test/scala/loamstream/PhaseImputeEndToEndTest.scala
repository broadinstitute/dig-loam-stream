package loamstream

import loamstream.model.HasAst
import loamstream.tools.core.CoreTool
import java.nio.file.Path
import java.io.File
import loamstream.model.AST
import loamstream.model.ToolSpec
import org.scalatest.FunSuite
import loamstream.conf.ImputationConfig
import com.typesafe.config.ConfigFactory
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
    
    val (toolbox, pipeline, executer) = doSetup(inputFile, output)
    
    val executable = toolbox.createExecutable(pipeline.ast)
    
    val results = executer.execute(executable)
    
    assert(results.size == 2)
    
    //Basically, see that each pipeline step finished with a non-zero status
    assert(results.values.forall {
      case Hit(r) => r.isSuccess
      case _ => false
    })
    
    //TODO: More; Ideally, we want to know we're computing the expected results 
  }
  
  private def doSetup(inputFile: Path, output: Path): (LToolBox, ShapeItImpute2Pipeline, LExecuter) = {
    
    val config = ImputationConfig.fromFile("src/test/resources/loamstream-test.conf").get
    
    val pipeline = ShapeItImpute2Pipeline(config, inputFile.toAbsolutePath, output.toAbsolutePath)
    
    val toolbox = CoreToolBox(LEnv.empty)
    
    val executer: LExecuter = ChunkedExecuter.default
    
    (toolbox, pipeline, executer)
  }
}

object PhaseImputeEndToEndTest {
  
  final case class ShapeItImpute2Pipeline(config: ImputationConfig, inputVcf: Path, outputFile: Path) extends HasAst {
    private def tempFile = File.createTempFile("shapeit", "loamstream").toPath.toAbsolutePath
    
    private lazy val shapeItTool = CoreTool.Phase(config.shapeIt, inputVcf, tempFile)
    
    private lazy val impute2Tool = CoreTool.Impute(config.impute2, shapeItTool.outputHaps, outputFile)
    
    override lazy val ast: AST = {
      import ToolSpec.ParamNames.{ input, output }
      
      AST(impute2Tool).get(input).from(AST(shapeItTool)(output))
    }
  }
}