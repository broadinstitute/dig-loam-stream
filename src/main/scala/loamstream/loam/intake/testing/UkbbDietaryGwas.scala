package loamstream.loam.intake.testing

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

import loamstream.conf.LoamConfig
import loamstream.loam.LoamProjectContext
import loamstream.loam.LoamScriptContext
import loamstream.loam.intake.aggregator
import loamstream.loam.intake.aggregator.AggregatorIntakeConfig
import loamstream.loam.intake.aggregator.Metadata
import loamstream.loam.intake.aggregator.SourceColumns

/**
 * @author clint
 * Feb 28, 2020
 */
object UkbbDietaryGwas extends loamstream.LoamFile {
  import loamstream.loam.intake.IntakeSyntax._
  
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
      strexpr"${CHR}_${BP}_${ALLELE1}_${ALLELE0}",
      //"{chrom}_{pos}_{alt}_{ref}"
      strexpr"${CHR}_${BP}_${ALLELE0}_${ALLELE1}")
        
    val a1Freq = A1FREQ.asDouble
    val beta = BETA.asDouble
    val zscore = BETA.asDouble / SE.asDouble
      
    val otherColumns = Seq(
      ColumnDef(aggregator.ColumnNames.pvalue, P_BOLT_LMM),
      ColumnDef(aggregator.ColumnNames.stderr, SE),
      ColumnDef(aggregator.ColumnNames.beta, beta, beta.negate),
      ColumnDef(aggregator.ColumnNames.eaf, a1Freq, 1.0 - a1Freq),
      ColumnDef(aggregator.ColumnNames.zscore, zscore, zscore.negate))
      
    UnsourcedRowDef(varId, otherColumns).from(source)
  }
  
  object Paths {
    val workDir: Path = path("./output")
    
    val baseDir: String = "/humgen/diabetes2/users/jcole/UKBB/diet/FFQ_GWAS_forT2Dportal_tmp"
    
    def dataFile(nameFragment: String): Path = {
      path(s"${baseDir}/BOLTlmm_UKB_genoQCEURN455146_v3_diet_${nameFragment}_BgenSnps_mac20_maf0.005_info0.6.gz")
    }
  }
  
  def sourceStore(phenotypeConfig: PhenotypeConfig): Store = {
    store(Paths.dataFile(phenotypeConfig.fileFragment))
  }
  
  def sourceStores(phenotypesToFileFragments: Map[String, PhenotypeConfig]): Map[String, Store] = {
    import loamstream.util.Maps.Implicits._
    
    phenotypesToFileFragments.strictMapValues(sourceStore(_).asInput)
  }
  
  def processPhenotype(
      phenotype: String, 
      sourceStore: Store,
      aggregatorIntakeConfig: AggregatorIntakeConfig,
      flipDetector: FlipDetector,
      destOpt: Option[Store] = None): Store = {
    
    require(sourceStore.isPathStore)
    
    val dest: Store = destOpt.getOrElse(store(Paths.workDir / s"ready-for-intake-${phenotype}.tsv"))
    
    val csvFormat = org.apache.commons.csv.CSVFormat.DEFAULT.withDelimiter(' ').withFirstRecordAsHeader
    
    val source = CsvSource.fromCommandLine(s"zcat ${sourceStore.path}", csvFormat)
    
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
  
  import loamstream.loam.intake.aggregator.AggregatorCommands.upload
  
  def toMetadata(phenotypeConfigTuple: (String, PhenotypeConfig)): Metadata = {
    val (phenotype, PhenotypeConfig(_, subjects)) = phenotypeConfigTuple
    
    generalMetadata.toMetadata(phenotype, Metadata.Quantitative.Subjects(subjects))
  }
  
  val flipDetector: FlipDetector = new FlipDetector.Default(
    referenceDir = aggregatorIntakePipelineConfig.genomeReferenceDir,
    isVarDataType = true,
    pathTo26kMap = aggregatorIntakePipelineConfig.twentySixKIdMap)
  
  for {
    (phenotype, sourceStore) <- sourceStores(phenotypesToConfigs)
  } {
    val phenotypeConfig = phenotypesToConfigs(phenotype)
    
    val dataInAggregatorFormat = processPhenotype(phenotype, sourceStore, aggregatorIntakePipelineConfig, flipDetector)
  
    if(intakeTypesafeConfig.getBoolean("AGGREGATOR_INTAKE_DO_UPLOAD")) {
      val metadata = toMetadata(phenotype -> phenotypeConfig)
      
      val sourceColumnMapping = SourceColumns.defaultMarkerAndPvalueOnly
        .withDefaultZscore
        .withDefaultStderr
        .withDefaultBeta
        .withDefaultEaf
      
      upload(
          aggregatorIntakePipelineConfig, 
          metadata, dataInAggregatorFormat, 
          sourceColumnMapping, 
          workDir = Paths.workDir, 
          yes = false).tag(s"upload-to-s3-${phenotype}")
    }
  }
}
