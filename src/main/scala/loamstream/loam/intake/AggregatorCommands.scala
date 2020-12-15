package loamstream.loam.intake

import java.nio.file.Paths

import loamstream.loam.LoamScriptContext
import loamstream.loam.LoamSyntax

/**
 * @author clint
 * Mar 2, 2020
 */
object AggregatorCommands extends AggregatorCommands

trait AggregatorCommands {
  import IntakeSyntax._
  import loamstream.loam.LoamSyntax._
  
  private def makeConfigFile(
      workDir: Path, 
      metadata: AggregatorMetadata,
      csvFile: Store,
      sourceColumnMapping: SourceColumns,
      forceLocal: Boolean)(implicit scriptContext: LoamScriptContext): Store = {
    
    val aggregatorConfigFileName: Path = {
      workDir.resolve(s"aggregator-intake-${metadata.dataset}-${metadata.phenotype}.conf")
    }
    
    val configData = AggregatorConfigData(metadata, sourceColumnMapping, csvFile.path)      
        
    val aggregatorConfigFile = store(aggregatorConfigFileName)
    
    produceAggregatorIntakeConfigFile(aggregatorConfigFile).
        from(configData, forceLocal).
        tag(s"make-aggregator-conf-${metadata.dataset}-${metadata.phenotype}")
        
    aggregatorConfigFile
  }
  
  def upload(
      aggregatorIntakeConfig: AggregatorIntakeConfig,
      metadata: AggregatorMetadata, 
      csvFile: Store, 
      sourceColumnMapping: SourceColumns,
      workDir: Path = Paths.get("."),
      logFile: Option[Path] = None,
      skipValidation: Boolean = false,
      forceLocal: Boolean = false,
      yes: Boolean = false)(implicit scriptContext: LoamScriptContext): Tool = {
    
    val aggregatorConfigFile = makeConfigFile(workDir, metadata, csvFile, sourceColumnMapping, forceLocal)
    
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
      
    def command = cmd"""${aggregatorIntakeCondaEnvPart}python ${mainPyPart} variants ${yesForcePart} ${skipValidationPart} ${aggregatorConfigFile} ${logfilePart}""". // scalastyle:ignore line.size.limit
        in(aggregatorConfigFile, csvFile)
        
    //TODO: HACK
    if(forceLocal) { local { command } } else { command }
  }
}
