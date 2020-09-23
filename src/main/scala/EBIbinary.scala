import loamstream.loam.LoamSyntax
import loamstream.loam.intake.IntakeSyntax
import loamstream.loam.LoamScriptContext
import loamstream.util.Maps
import loamstream.loam.intake.aggregator.{ ColumnNames => AggregatorColumnNames }
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
import loamstream.loam.intake.aggregator.SourceColumns

object EBIbinary extends loamstream.LoamFile {
  import IntakeSyntax._

  object ColumnNames {
    val CHROM = "CHROM".asColumnName
    val POS = "POS".asColumnName
    val REF = "REF".asColumnName
    val ALT = "ALT".asColumnName
    val EAF = "EAF".asColumnName
    val ODDS_RATIO = "ODDS_RATIO".asColumnName
    val PVALUE = "PVALUE".asColumnName
  }
  
  def rowDef(source: CsvSource): RowDef = {
    import ColumnNames._

	val filtered = source.filter(ODDS_RATIO.asDouble > 0.0)
    
    val varId = ColumnDef(
      AggregatorColumnNames.marker, 
      //"{chrom}_{pos}_{ref}_{alt}"
      strexpr"${CHROM}_${POS}_${REF}_${ALT}",
      //"{chrom}_{pos}_{alt}_{ref}"
      strexpr"${CHROM}_${POS}_${ALT}_${REF}")
        
    val eaf = EAF.asDouble
    val odds_ratio = ODDS_RATIO.asDouble

    val neff = ColumnName("Neff")
    val n = ColumnName("n")
      
    val otherColumns = Seq(
      ColumnDef(AggregatorColumnNames.pvalue, PVALUE),
      ColumnDef(n,neff),
      ColumnDef(AggregatorColumnNames.odds_ratio, odds_ratio, 1.0 / odds_ratio),
      ColumnDef(AggregatorColumnNames.eaf, eaf, 1.0 - eaf))
      
    UnsourcedRowDef(varId, otherColumns).from(filtered)
  }

  object Paths {
    //val workDir: Path = path("./to_aggregator")
    val workDir: Path = path("./")
    
    val baseDir: String = "/humgen/diabetes2/users/ryank/portals/upload/T2DKP/EBI/from_EBI/no_missing"
    
    def dataFile(nameFragment: String): Path = {
      path(s"${baseDir}/${nameFragment}.neff.tsv.gz")
    }
  }
  
  def sourceStore(phenotypeConfig: PhenotypeConfig): Store = {
    store(Paths.dataFile(phenotypeConfig.fileFragment))
  }
  
  def sourceStores(phenotypesToFileFragments: Map[String, PhenotypeConfig]): Map[String, Store] = {
    import Maps.Implicits._
    
    phenotypesToFileFragments.strictMapValues(sourceStore(_).asInput)
  }

  private val intakeTypesafeConfig: Config = loadConfig("INTAKE_CONF", "").config
  
  private val intakeMetadataTypesafeConfig: Config = loadConfig("INTAKE_METADATA_CONF", "").config
  
  def processPhenotype(
      dataset: String,
      phenotype: String, 
      sourceStore: Store,
      aggregatorIntakeConfig: AggregatorIntakeConfig,
      flipDetector: FlipDetector,
      destOpt: Option[Store] = None): Store = {
    
    require(sourceStore.isPathStore)
    
    val dest: Store = destOpt.getOrElse(store(Paths.workDir / s"""${dataset}_${phenotype}.tsv"""))
    
    val csvFormat = org.apache.commons.csv.CSVFormat.DEFAULT.withDelimiter('\t').withFirstRecordAsHeader
    
    val source = CsvSource.fromCommandLine(s"zcat ${sourceStore.path}", csvFormat)
    
    val columns = rowDef(source)
        
    produceCsv(dest).
        from(columns).
        using(flipDetector).
        tag(s"process-phenotype-$phenotype").
        in(sourceStore)
        
    dest
  }

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
  
  val flipDetector: FlipDetector = new FlipDetector.Default(
    referenceDir = aggregatorIntakePipelineConfig.genomeReferenceDir,
    isVarDataType = true,
    pathTo26kMap = aggregatorIntakePipelineConfig.twentySixKIdMap)
  
  for {
    (phenotype, sourceStore) <- sourceStores(phenotypesToConfigs)
  } {
    val phenotypeConfig = phenotypesToConfigs(phenotype)
    
    val dataInAggregatorFormat = processPhenotype(generalMetadata.dataset, phenotype, sourceStore, aggregatorIntakePipelineConfig, flipDetector)
    
    if(intakeTypesafeConfig.getBoolean("AGGREGATOR_INTAKE_DO_UPLOAD")) {
      val metadata = toMetadata(phenotype -> phenotypeConfig)
    
      val sourceColumnMapping = SourceColumns.defaultMarkerAndPvalueOnly
        .withDefaultOddsRatio
        .withDefaultEaf
      
      upload(
          aggregatorIntakePipelineConfig, 
          metadata, 
          dataInAggregatorFormat,
          sourceColumnMapping,
          workDir = Paths.workDir, 
          yes = true).tag(s"upload-to-s3-${phenotype}")
    }
  }
}
