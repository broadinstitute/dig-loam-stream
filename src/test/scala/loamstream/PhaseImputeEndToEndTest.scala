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

/**
 * @author clint
 * date: Jun 13, 2016
 */
final class PhaseImputeEndToEndTest extends FunSuite {
  import PhaseImputeEndToEndTest._
  
  test("Run shapeit, then impute2 on example data") {
    
  }
}

object PhaseImputeEndToEndTest {
  final case class ShapeItImpute2Pipeline(inputVcf: Path, outputFile: Path) extends HasAst {
    private def tempFile = File.createTempFile("shapeit", "loamstream").toPath
    
    private lazy val config = ImputationConfig.fromConfig(ConfigFactory.load()).get
    
    private lazy val shapeItTool = CoreTool.Phase(config.shapeIt, inputVcf, tempFile)
    
    private lazy val impute2Tool = CoreTool.Impute(config.impute2, shapeItTool.outputVcf, outputFile)
    
    override lazy val ast: AST = {
      import ToolSpec.ParamNames.{ input, output }
      
      AST(impute2Tool).get(input).from(AST(shapeItTool)(output))
    }
  }
}