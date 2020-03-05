package loamstream.loam.intake.testing

import loamstream.loam.LoamSyntax
import loamstream.loam.intake.IntakeSyntax
import loamstream.loam.LoamScriptContext
import loamstream.loam.intake.CsvSource
import loamstream.util.Maps
import loamstream.loam.intake.UnsourcedColumnDef
import loamstream.loam.intake.FlipDetector
import loamstream.loam.intake.RowDef
import loamstream.loam.intake.UnsourcedRowDef
import loamstream.loam.intake.aggregator
import loamstream.loam.LoamCmdSyntax
import loamstream.loam.intake.aggregator.Metadata
import loamstream.loam.intake.aggregator.AggregatorIntakeConfig
import loamstream.util.Traversables
import com.typesafe.config.Config
import loamstream.conf.DataConfig
import loamstream.loam.intake.aggregator.AggregatorCommands
import loamstream.loam.LoamProjectContext
import loamstream.conf.LoamConfig
import com.typesafe.config.ConfigFactory

/**
 * @author clint
 * Feb 28, 2020
 */
object UkbbDietaryGwas extends App {
  import LoamSyntax._
  import IntakeSyntax._
  implicit val scriptContext: LoamScriptContext = new LoamScriptContext(LoamProjectContext.empty(LoamConfig.fromConfig(ConfigFactory.load()).get))
  
  object ColumnNames {
    val CHR = "CHR".asColumnName
    val BP = "BP".asColumnName
    val ALLELE1 = "ALLELE1".asColumnName
    val ALLELE0 = "ALLELE0".asColumnName
    val A1FREQ = "A1FREQ".asColumnName 
    val INFO = "INFO".asColumnName
    val BETA = "BETA".asColumnName
    val SE = "SE".asColumnName
    val P_BOLT_LMM = "P_BOLT_LMM".asColumnName
    
    val VARID = "VARID".asColumnName
  }
  
  def rowDef(source: CsvSource): RowDef = {
    import ColumnNames._
    
    val varId = ColumnDef(
      aggregator.ColumnNames.marker, 
      //"{chrom}_{pos}_{ref}_{alt}"
      strexpr"${CHR}_${BP}_${ALLELE0}_${ALLELE1}",
      //"{chrom}_{pos}_{alt}_{ref}"
      strexpr"${CHR}_${BP}_${ALLELE1}_${ALLELE0}")
        
    val otherColumns = Seq(
      ColumnDef(aggregator.ColumnNames.pvalue, P_BOLT_LMM),
      ColumnDef(aggregator.ColumnNames.stderr, SE),
      ColumnDef(aggregator.ColumnNames.beta, BETA),
      ColumnDef(aggregator.ColumnNames.eaf, A1FREQ),
      ColumnDef(aggregator.ColumnNames.zscore, BETA.asDouble / SE.asDouble))
      
    UnsourcedRowDef(varId, otherColumns).from(source)
  }
  
  object Paths {
    val baseDir: String = "/humgen/diabetes2/users/jcole/UKBB/diet/FFQ_GWAS_forT2Dportal_tmp"
    
    def dataFile(nameFragment: String): Path = {
      path(s"${baseDir}/BOLTlmm_UKB_genoQCEURN455146_v3_diet_${nameFragment}_BgenSnps_mac20_maf0.005_info0.6.gz")
    }
  }
  
  def sourceStore(phenotypeConfig: PhenotypeConfig): Store = {
    store(Paths.dataFile(phenotypeConfig.fileFragment))
  }
  
  def sourceStores(phenotypesToFileFragments: Map[String, PhenotypeConfig]): Map[String, Store] = {
    import Maps.Implicits._
    
    phenotypesToFileFragments.strictMapValues(sourceStore(_).asInput)
  }
  
  def processPhenotype(
      phenotype: String, 
      sourceStore: Store,
      aggregatorIntakeConfig: AggregatorIntakeConfig,
      flipDetector: FlipDetector,
      destOpt: Option[Store] = None): Store = {
    
    require(sourceStore.isPathStore)
    
    val dest: Store = destOpt.getOrElse(store(s"ready-for-intake-${phenotype}.tsv"))
    
    val source = CsvSource.fromCommandLine(s"zcat ${sourceStore.path}")
    
    val columns = rowDef(source)
        
    produceCsv(dest).
        from(columns).
        using(flipDetector).
        tag(s"process-phenotype-$phenotype").
        in(sourceStore)
        
    dest
  }
  
  private val intakeTypesafeConfig: Config = loadConfig("INTAKE_CONF", "").config
  
  private val intakeMetadataTypesafeConfig: Config = loadConfig("INTAKE_METADATA_CONF", "").config
  
  val generalMetadata: Metadata.NoPhenotypeOrQuantitative = {
    Metadata.NoPhenotypeOrQuantitative.fromConfig(intakeMetadataTypesafeConfig).get
  }
  
  val aggregatorIntakePipelineConfig: AggregatorIntakeConfig = {
    AggregatorIntakeConfig.fromConfig(intakeTypesafeConfig).get
  }
  
  final case class PhenotypeConfig(fileFragment: String, subjects: Int)
  
  val phenotypesToConfigs: Map[String, PhenotypeConfig] = {
    val key = "loamstream.aggregator.intake.phenotypesToFileFragments"
    
    import net.ceedubs.ficus.Ficus._
    import net.ceedubs.ficus.readers.ArbitraryTypeReader._
    
    intakeTypesafeConfig.as[Map[String, PhenotypeConfig]](key)
  }
  
  import AggregatorCommands.upload
  
  def toMetadata(phenotypeConfigTuple: (String, PhenotypeConfig)): Metadata = {
    val (phenotype, PhenotypeConfig(_, subjects)) = phenotypeConfigTuple
    
    generalMetadata.toMetadata(phenotype, Metadata.Quantitative.Subjects(subjects))
  }
  
  val flipDetector: FlipDetector = new FlipDetector(
    referenceDir = aggregatorIntakePipelineConfig.genomeReferenceDir,
    isVarDataType = true,
    pathTo26kMap = aggregatorIntakePipelineConfig.twentySixKIdMap)
  
  for {
    (phenotype, sourceStore) <- sourceStores(phenotypesToConfigs)
  } {
    val phenotypeConfig = phenotypesToConfigs(phenotype)
    
    val dataInAggregatorFormat = processPhenotype(phenotype, sourceStore, aggregatorIntakePipelineConfig, flipDetector)
  
    val metadata = toMetadata(phenotype -> phenotypeConfig)
    
    upload(aggregatorIntakePipelineConfig, metadata, dataInAggregatorFormat, yes = false).
      tag(s"upload-to-s3-${phenotype}")
  }
}
