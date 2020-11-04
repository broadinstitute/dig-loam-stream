package loamstream.loam.intake.examples

import com.typesafe.config.ConfigFactory
import loamstream.loam.intake.IntakeSyntax
import loamstream.loam.LoamScriptContext
import loamstream.loam.LoamProjectContext
import loamstream.loam.LoamSyntax
import loamstream.conf.LoamConfig
import loamstream.loam.intake.AggregatorCommands
import loamstream.loam.intake.AggregatorIntakeConfig
import loamstream.loam.intake.SourceColumns
import loamstream.conf.LsSettings

/**
 * @author clint
 * Apr 24, 2020
 * 
 * An annotated example illustrating the proof-of-concept intake Loam DSL.
 * 
 * It encodes processing an input .tsv and uploading it to S3 via the aggregator-intake project.  
 */
object ReadMe extends loamstream.LoamFile with AggregatorCommands {
  import IntakeSyntax._

  /*
   * 1)
   * 
   * CsvSources are handles to CSV data that may be iterated over:   
   */
  {
    val someSource: Source[CsvRow] = Source.fromFile(path("some-file.tsv"))
    
    val rows: Iterator[CsvRow] = someSource.records
    
    /*
     * You can filter over the Iterators you get from CsvSources, or you can also get a filtered
     * CsvSource that will always give you filtered Iterators:  
     */
    val filteredSource: Source[CsvRow] = someSource.filter(ColumnNames.BETA.asDouble > 42.0)
    
    //Similarly, you can limit the size of CsvSources or skip leading rows:
    val first10Source: Source[CsvRow] = someSource.take(10)
    val skipsFirstRowSource: Source[CsvRow] = someSource.drop(1)
  }
  
  /*
   * 2) 
   * 
   * A CsvRow is a abstract way to get the values from columns in a row from a CSV file, either by index or by name.
   * 
   * Given CSV data that looks like in some-file.tsv,
   * 
   * FOO BAR BAZ
   * x   42  hello
   * y   123 world
   * z   13  asdf
   */
  val firstRow: CsvRow = Source.fromFile(path("some-file.tsv")).records.next()
  val secondRow: CsvRow = Source.fromFile(path("some-file.tsv")).drop(1).records.next()
  
  secondRow.getFieldByIndex(0) // => "y"
  secondRow.getFieldByName("BAR") // => "123"
  secondRow.getFieldByIndex(2) // => "world"
  
  /*
   * 2) 
   * 
   * A ColumnExpr[A] is a function from a CsvRow to an A.
   * 
   * ColumnNames are ColumnExpr[String]s that produce the value from the column with a given name:
   */
  val BAR = ColumnName("BAR") //or "BAR".asColumnName
    
  val itReallyIsAFunction: CsvRow => String = BAR
  
  val barValue = BAR.apply(secondRow) //"123"
  
  /*
   * ColumnExprs can be composed with lots of combinators:
   */
  {
    val composedExpr: ColumnExpr[Double] = -(BAR.asDouble.map(_ + 1) * 2.0)
    
    composedExpr.apply(firstRow) // -((42.0 + 1) * 2) == -86.0
  }
  
  /*
   * ColumnExprs produce values; combining a ColumnExpr with a name gives you a ColumnDef. ColumnDefs aren't 
   * literally Functions like ColumnExprs are, but conceptually they are; they're a way to produce a named 
   * output column from an input row.
   */
  val BIP = "BIP".asColumnName
  //Make a column named 'BIP' that has values that are the ones from column BAR plus one:
  NamedColumnDef(BIP, BAR.asInt + 1)
  //Make it BAR * 2 when it's detected that ref and alt alleles are flipped, otherwise BAR + 1:
  NamedColumnDef(BIP, BAR.asInt + 1, BAR.asInt * 2)

  /*
   * Since ColumnDefs produce named columns, combining a bunch of them makes a row.  That's what a RowDef
   * is for.  A varId/marker ColumnDef is required, plus zero or more other ColumnDefs for other columns:
   */
  
  object ColumnNames {
    val VAR_ID = "VAR_ID".asColumnName
    val CHR = "CHR".asColumnName
    val POS = "POS".asColumnName
    val BP = "BP".asColumnName
    val ALLELE0 = "ALLELE0".asColumnName
    val ALLELE1 = "ALLELE1".asColumnName
    val EAF_PH = "EAF_PH".asColumnName
    val MAF_PH = "MAF_PH".asColumnName
    val A1FREQ = "A1FREQ".asColumnName
    val BETA = "BETA".asColumnName
    val SE = "SE".asColumnName
    val P_VALUE = "P_VALUE".asColumnName
    val P_BOLT_LMM = "P_BOLT_LMM".asColumnName
  }
  
  //Helpers that encode common patterns 
  import AggregatorColumnDefs.{marker, eaf, beta, just, pvalue, stderr}
  import AggregatorColumnNames._
  
  //Use aggregator-default column names to make things easier
  //TODO: UPDATE
  /*val rowDef = RowDef(
      varIdDef = marker(CHR, BP, ALLELE0, ALLELE1),
      otherColumns = Seq(
        eaf(A1FREQ),
        beta(BETA),
        stderr(SE),
        pvalue(P_BOLT_LMM)))*/
        
  //Implements heuristics to tell if ref and alt alleles are flipped
  val flipDetector: FlipDetector = new FlipDetector.Default(
        referenceDir = path("src/test/resources/intake/reference-first-1M-of-chrom1"),
        isVarDataType = true,
        pathTo26kMap = path("src/test/resources/intake/26k_id.map.first100"))
  
  val input = Source.fromFile(
        path("src/test/resources/intake/real-input-data.tsv"), 
        Source.Formats.spaceDelimitedWithHeader)
  
  val transformedCsv = store("target/ReadMe/transformed.tsv")
  
  //What follows is still rough, and could use some factoring
  
  //TODO: UPDATE
  /*produceCsv(transformedCsv).
    from(rowDef .from(input) ).
    using(flipDetector).
    tag("makeCsv")*/
  
  /*
   * Values that work when running as diguser:
   * condaEnvName = Some("intake")
   * scriptsRoot = path("/humgen/diabetes/users/dig/aggregator/git-clones/dig-aggregator-intake")
   * condaExecutable = path("/humgen/diabetes/users/dig/miniconda3/condabin/conda")
   * genomeReferenceDir = path("/humgen/diabetes2/users/mvg/portal/scripts/reference")
   * twentySixKIdMap = path("/humgen/diabetes2/users/mvg/portal/scripts/26k_id.map")
   */
  val aggregatorConfig = AggregatorIntakeConfig(
    condaEnvName = Some("intake"),
    scriptsRoot = path("/path/to/git/clone/of/dig-aggregator-intake"),
    condaExecutable = path("/path/to/whatever/bin/conda"),
    genomeReferenceDir = path("/path/to/dir/containing/reference/genome/files"),
    twentySixKIdMap = path("/paht/to/26k_id.map"))

  val metadata: AggregatorMetadata = AggregatorMetadata(
    dataset = "dataset-name",
    phenotype = "some-phenotype",
    //See https://github.com/broadinstitute/dig-aggregator-intake
    varIdFormat = "defaults to {chrom}_{pos}_{ref}_{alt}",
    ancestry = "some-ancestry",
    author = Some("John Doe"),
    tech = "some-tech",
    quantitative = Some(AggregatorMetadata.Quantitative.Subjects(42))
    //or quantitative = Some(Metadata.Quantitative.CasesAndControls(cases = 42, controls = 99))
  )
  
  //NB: both aggregator.AggregatorIntakeConfigs and aggregator.Metadatas can be unmarshalled from HOCON .conf files.
  //See aggregator.{AggregatorIntakeConfig,Metadata}.fromConfig(Config)
    
  val reallyProceed = false
  
  val sourceColumnMapping = {
    SourceColumns.defaultMarkerAndPvalueOnly
      .withDefaultEaf  
      .withDefaultBeta
      .withDefaultStderr
  }
  
  upload(
      aggregatorIntakeConfig = aggregatorConfig,// aggregator-intake install dir, conda env name, etc 
      metadata = metadata, //Aggregator-specific metadata - dataset/phenotype name , num cases/controls, etc etc
      csvFile = transformedCsv,
      sourceColumnMapping = sourceColumnMapping,
      yes = reallyProceed).tag("upload-to-S3")
}
