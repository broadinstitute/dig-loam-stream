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
import loamstream.model.execute.LeavesFirstExecuter
import loamstream.util.Hit

/**
 * @author clint
 * date: Jun 13, 2016
 */
final class PhaseImputeEndToEndTest extends FunSuite {
  import PhaseImputeEndToEndTest._
  
  test("Run shapeit, then impute2 on example data") {
    val (toolbox, pipeline) = makeToolBoxAndPipeline()
    
    val executer = {
      import scala.concurrent.ExecutionContext.Implicits.global
      
      new LeavesFirstExecuter
    }
    
    val executable = toolbox.createExecutable(pipeline.ast)
    
    val results = executer.execute(executable)
    
    println(results)
    
    assert(results.size == 2)
    
    assert(results.values.forall {
      case Hit(r) => r.isSuccess
      case _ => false
    })
    
    
  }
  
  
  private def makeToolBoxAndPipeline(): (LToolBox, ShapeItImpute2Pipeline) = {

    val input = Paths.get("src/test/resources/imputation/gwas.vcf.gz").toAbsolutePath
    
    val output = Paths.get("target/bar")
    
    val config = ImputationConfig.fromFile("src/test/resources/loamstream-test.conf").get
    
    val pipeline = ShapeItImpute2Pipeline(config, input, output)
    
    val toolbox = CoreToolBox(LEnv.empty)
    
    (toolbox, pipeline)
  }
}

object PhaseImputeEndToEndTest {
  
  final case class ShapeItImpute2Pipeline(config: ImputationConfig, inputVcf: Path, outputFile: Path) extends HasAst {
    private def tempFile = File.createTempFile("shapeit", "loamstream").toPath
    
    private lazy val shapeItTool = CoreTool.Phase(config.shapeIt, inputVcf, tempFile)
    
    private lazy val impute2Tool = CoreTool.Impute(config.impute2, shapeItTool.outputHaps, outputFile)
    
    override lazy val ast: AST = {
      import ToolSpec.ParamNames.{ input, output }
      
      AST(impute2Tool).get(input).from(AST(shapeItTool)(output))
    }
  }
}