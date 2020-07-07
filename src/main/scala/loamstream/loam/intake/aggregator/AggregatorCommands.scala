package loamstream.loam.intake.aggregator

import loamstream.model.Store
import loamstream.loam.LoamScriptContext
import loamstream.loam.LoamSyntax
import loamstream.loam.LoamCmdSyntax
import loamstream.loam.intake.IntakeSyntax
import java.nio.file.Paths

/**
 * @author clint
 * Mar 2, 2020
 */
object AggregatorCommands extends AggregatorCommands

trait AggregatorCommands {
  import LoamSyntax._
  import IntakeSyntax._
  
  def upload(
      aggregatorIntakeConfig: AggregatorIntakeConfig,
      metadata: Metadata, 
      csvFile: Store, 
      sourceColumnMapping: SourceColumns,
      workDir: Path = Paths.get("."),
      skipValidation: Boolean = false,
      yes: Boolean = false)(implicit scriptContext: LoamScriptContext): Tool = {
    
    val aggregatorConfigFileName: Path = {
      workDir.resolve(s"aggregator-intake-${metadata.dataset}-${metadata.phenotype}.conf")
    }
    
    val configData = ConfigData(metadata, sourceColumnMapping, csvFile.path)      
        
    val aggregatorConfigFile = store(aggregatorConfigFileName)
    
    produceAggregatorIntakeConfigFile(aggregatorConfigFile).
        from(configData).
        tag(s"make-aggregator-conf-${metadata.dataset}-${metadata.phenotype}").
        in(csvFile)
    
    val aggregatorIntakeCondaEnvPart = aggregatorIntakeConfig.condaEnvName.map { condaEnv =>
      val condaExecutable = aggregatorIntakeConfig.condaExecutable
    
      val envNamePart = s"-n ${condaEnv}"
      
      s"${condaExecutable} run ${envNamePart} "
    }.getOrElse("")
    
      val aggregatorIntakeScriptsRoot = aggregatorIntakeConfig.scriptsRoot
    
    val mainPyPart = s"${aggregatorIntakeScriptsRoot}/main.py"
    
    val yesForcePart = if(yes) "--yes --force" else ""
      
    val skipValidationPart = if(skipValidation) "--skip-validation" else ""
      
    cmd"""${aggregatorIntakeCondaEnvPart}python ${mainPyPart} variants ${yesForcePart} ${skipValidationPart} ${aggregatorConfigFile}""". // scalastyle:ignore line.size.limit
        in(aggregatorConfigFile, csvFile)
  }
}
