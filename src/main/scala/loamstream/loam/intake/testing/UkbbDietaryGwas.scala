package loamstream.loam.intake.testing

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

import loamstream.conf.LoamConfig
import loamstream.loam.LoamProjectContext
import loamstream.loam.LoamScriptContext
import loamstream.loam.intake.AggregatorIntakeConfig
import loamstream.loam.intake.SourceColumns
import loamstream.loam.intake.RowSink
import loamstream.loam.intake.AggregatorCommands.upload

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
    val Z_SCORE = "Z_SCORE".asColumnName
    
    val VARID = "VARID".asColumnName
  }

  val toAggregatorRows: AggregatorRowExpr = {
    import ColumnNames._
    import AggregatorColumnDefs._
    
    AggregatorRowExpr(
      markerDef = marker(
          chromColumn = CHR, 
          posColumn = BP, 
          refColumn = ALLELE1, 
          altColumn = ALLELE0),
      pvalueDef = pvalue(P_BOLT_LMM),
      //zscoreDef = Some(zscore(BETA, SE)),
      //zscoreDef = Some(zscore(Z_SCORE)),
      zscoreDef = Some(PassThru.zscore(Z_SCORE)),
      stderrDef = Some(stderr(SE)),
      betaDef = Some(beta(BETA)),
      eafDef = Some(eaf(A1FREQ)))
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
    
    val destPath = Paths.workDir / s"ready-for-intake-${phenotype}.tsv"
    
    val dest: Store = destOpt.getOrElse(store(destPath))
    
    val csvFormat = org.apache.commons.csv.CSVFormat.DEFAULT.withDelimiter(' ').withFirstRecordAsHeader
    
    val source = Source.fromCommandLine(s"zcat ${sourceStore.path}", csvFormat)
    
    val filterLog: Store = store(path(s"${dest.path.toString}.filtered-rows"))
    val unknownToBioIndexFile: Store = store(path(s"${dest.path.toString}.unknown-to-bio-index"))
    val disagreeingZBetaStdErrFile: Store = store(path(s"${dest.path.toString}.disagreeing-z-Beta-stderr"))
    
    produceCsv(dest).
        from(source).
        using(flipDetector).
        via(toAggregatorRows).
        filter(DataRowFilters.validEaf(filterLog, append = true)).
        filter(DataRowFilters.validMaf(filterLog, append = true)).
        map(DataRowTransforms.clampPValues).
        filter(DataRowFilters.validPValue(filterLog, append = true)).
        withMetric(Metrics.fractionUnknownToBioIndex(unknownToBioIndexFile)).
        withMetric(Metrics.fractionWithDisagreeingBetaStderrZscore(disagreeingZBetaStdErrFile, flipDetector)).
        write().
        tag(s"process-phenotype-$phenotype").
        in(sourceStore).
        out(dest).
        out(unknownToBioIndexFile).
        out(disagreeingZBetaStdErrFile)
       
    dest
  }
  
  private val intakeTypesafeConfig: Config = loadConfig("INTAKE_CONF", "").config
  
  private val intakeMetadataTypesafeConfig: Config = loadConfig("INTAKE_METADATA_CONF", "").config
  
  val generalMetadata: AggregatorMetadata.NoPhenotypeOrQuantitative = {
    AggregatorMetadata.NoPhenotypeOrQuantitative.fromConfig(intakeMetadataTypesafeConfig).get
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
  
  def toMetadata(phenotypeConfigTuple: (String, PhenotypeConfig)): AggregatorMetadata = {
    val (phenotype, PhenotypeConfig(_, subjects)) = phenotypeConfigTuple
    
    generalMetadata.toMetadata(phenotype, Some(AggregatorMetadata.Quantitative.Subjects(subjects)))
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
