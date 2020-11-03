package loamstream.loam.intake

import loamstream.loam.LoamScriptContext
import java.nio.file.Paths
import loamstream.loam.LoamSyntax

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
      metadata: AggregatorMetadata, 
      csvFile: Store, 
      sourceColumnMapping: SourceColumns,
      workDir: Path = Paths.get("."),
      logFile: Option[Path] = None,
      skipValidation: Boolean = false,
      yes: Boolean = false)(implicit scriptContext: LoamScriptContext): Tool = {
    
    val aggregatorConfigFileName: Path = {
      workDir.resolve(s"aggregator-intake-${metadata.dataset}-${metadata.phenotype}.conf")
    }
    
    val configData = AggregatorConfigData(metadata, sourceColumnMapping, csvFile.path)      
        
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
      
    val logfilePart: String = logFile.map(lf => s"2>&1 | tee ${lf}").getOrElse("")
      
    cmd"""${aggregatorIntakeCondaEnvPart}python ${mainPyPart} variants ${yesForcePart} ${skipValidationPart} ${aggregatorConfigFile} ${logfilePart}""". // scalastyle:ignore line.size.limit
        in(aggregatorConfigFile, csvFile)
  }
}
