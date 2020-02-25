package loamstream.loam.intake

import org.scalatest.FunSuite
import loamstream.TestHelpers
import loamstream.loam.LoamSyntax
import loamstream.loam.intake.CsvSource.FastCsv.FromCommand
import loamstream.model.execute.RxExecuter
import loamstream.compiler.LoamEngine
import loamstream.util.Files
import loamstream.loam.LanguageSupport
import loamstream.model.jobs.JobResult
import com.dimafeng.testcontainers.ForAllTestContainer
import com.dimafeng.testcontainers.MySQLContainer
import com.dimafeng.testcontainers.Container
import java.nio.file.Path
import java.net.URI
import org.broadinstitute.dig.aws.AWS
import org.broadinstitute.dig.aws.config.AWSConfig
import org.broadinstitute.dig.aws.config.S3Config
import org.broadinstitute.dig.aws.config.emr.EmrConfig
import org.broadinstitute.dig.aws.config.emr.SubnetId
import org.broadinstitute.dig.aws.config.emr.SubnetId
import java.io.StringReader
import loamstream.util.Traversables

/**
 * @author clint
 * Dec 20, 2019
 */
final class CsvTransformationTest extends AggregatorIntakeTest {
  
  import CsvTransformationTest._
  
  test("End-to-end CSV munging") {
    makeTablesAndThen {
      TestHelpers.withWorkDir(getClass.getSimpleName) { workDir =>
        val paths = new Paths(workDir)
        
        val metadata: aggregator.Metadata = aggregator.Metadata(
          dataset = "some-dataset",
          phenotype = "some-phenotype",
          ancestry = "some-ancestry",
          author = Some("some-author"),
          tech = "some-tech",
          cases = 42,
          controls = 21,
          varIdFormat = Some("{chrom}_{pos}_{ref}_{alt}"))  
        
        import paths._
        
        val graph = TestHelpers.makeGraph { implicit scriptContext =>
          import LoamSyntax._
          import IntakeSyntax._
          
          val inputDataFile = store(path("src/test/resources/intake-data.txt")).asInput
          val mungedDataFile = store(mungedOutputPath)
          val schemaFile = store(schemaFilePath)
          val dataListFile = store(dataListFilePath)
          val schemaListFile = store(schemaListFilePath)
          
          val columns: Seq[UnsourcedColumnDef] = {
            import Loam.ColumnNames._
            
            Seq(
              ColumnDef(
                VARID,
                strexpr"${CHROM}_${POS}_${Allele2.asUpperCase}_${Allele1.asUpperCase}",
                strexpr"${CHROM}_${POS}_${Allele1.asUpperCase}_${Allele2.asUpperCase}"),
              ColumnDef(CHROM),
              ColumnDef(POS),
              ColumnDef(ReferenceAllele, Allele2.asUpperCase, Allele1.asUpperCase),
              ColumnDef(EffectAllele, Allele1.asUpperCase, Allele2.asUpperCase),
              ColumnDef(EffectAllelePH, Allele1.asUpperCase, Allele2.asUpperCase),
              ColumnDef(EAF, Freq1.asDouble, Freq1.asDouble.complement),
              ColumnDef(EAFPH, Freq1.asDouble, Freq1.asDouble.complement),
              ColumnDef(MAF, Freq1.asDouble.complementIf(_ > 0.5)),
              ColumnDef(MAFPH, Freq1.asDouble.complementIf(_ > 0.5)),
              ColumnDef(OddsRatio, Effect.asDouble.exp, Effect.asDouble.negate.exp),
              ColumnDef(SE, StdErr.asDouble, StdErr.asDouble),
              ColumnDef(PValue, PDashValue.asDouble, PDashValue.asDouble))
          }
          
          val source: CsvSource = FromCommand(s"cat ${inputDataFile.path}")
          
          val flipDetector = new FlipDetector(
            referenceDir = path("/home/clint/workspace/marcins-scripts/reference"),
            isVarDataType = true,
            pathTo26kMap = path("/home/clint/workspace/marcins-scripts/26k_id.map"))
          
          val varIdColumn +: otherColumns = source.producing(columns)
          
          produceCsv(mungedDataFile).
              from(varIdColumn, otherColumns: _*).
              using(flipDetector).
              tag("makeCSV").
              in(inputDataFile)
          
          produceSchemaFile(schemaFile).
              from(columns: _*).
              tag("makeSchemaFile")
          
          produceListFiles(dataListFile, schemaListFile).
              from(mungedDataFile, schemaFile).
              tag("makeListFiles").
              in(mungedDataFile, schemaFile)
          
          val aggregatorConfigFile = store(aggregatorConfigFilePath)
          
          val sourceColumns = aggregator.SourceColumns(
              marker = Loam.ColumnNames.VARID,
              pValue = Loam.ColumnNames.PValue,
              zScore = Loam.ColumnNames.OddsRatio,
              stderr = Loam.ColumnNames.SE,
              beta = Loam.ColumnNames.OddsRatio,
              oddsRatio = Loam.ColumnNames.OddsRatio,
              eaf = Loam.ColumnNames.EAF,
              maf = Loam.ColumnNames.MAF)
              
          val configData = aggregator.ConfigData(metadata, sourceColumns, mungedDataFile.path)      
              
          produceAggregatorIntakeConfigFile(aggregatorConfigFile).
              from(configData).
              tag("make-aggregator-conf").
              in(mungedDataFile)
           
          val aggregatorEnvFile = store(aggregatorEnvFilePath)
              
          produceAggregatorEnvFile(aggregatorEnvFile, s3Bucket).tag("make-env-file")
              
          val upload = {
            //TODO
            val aggregatorIntakeCondaEnv = "intake"
            //TODO
            val aggregatorIntakeScriptsRoot = "~/workspace/dig-aggregator-intake"
            
            val mainPyPart = s"${aggregatorIntakeScriptsRoot}/main.py"
            val envNamePart = s"-n ${aggregatorIntakeCondaEnv}"
            
            cmd"""/opt/miniconda3/condabin/conda run ${envNamePart} python ${mainPyPart} variants --yes --force --skip-validation ${aggregatorConfigFile}""". // scalastyle:ignore line.size.limit
                in(aggregatorEnvFile, aggregatorConfigFile, mungedDataFile).
                tag("upload-to-s3")
          }
        }
        
        deleteUploadedData(metadata)
        
        val executer = RxExecuter.default
        val executable = LoamEngine.toExecutable(graph)
        
        val results = executer.execute(executable)
        
        val (uploadJob, uploadExecution) = results.find { case (j, _) => j.name == "upload-to-s3" }.get
        
        assert(uploadExecution.result.get.isSuccess)
        
        assert(results.size === 6)
        assert(results.values.forall(_.isSuccess))
            
        assert(Files.readFrom(mungedOutputPath) === expectedMungedContents)
        
        assert(Files.readFrom(schemaFilePath) === expectedSchemaFileContents)
        
        assert(Files.readFrom(dataListFilePath) === s"${mungedOutputPath.toString}${System.lineSeparator}")
        assert(Files.readFrom(schemaListFilePath) === s"${schemaFilePath.toString}${System.lineSeparator}")
        
        assert(Files.readFrom(aggregatorConfigFilePath) === expectedConfigFileContents(mungedOutputPath))
        
        validateAggregatorEnvFile(aggregatorEnvFilePath, s3Bucket)
        
        validateUploadedData(metadata)
      }
    }
  }
  
  private val s3Bucket = "dig-integration-tests"
  
  private val awsConfig = AWSConfig(
        S3Config(bucket = s3Bucket), 
        EmrConfig(sshKeyName = "whatever", subnetId = SubnetId("subnet-lalala")))
  
  private val aws = new AWS(awsConfig)
  
  private def validateAggregatorEnvFile(envFile: Path, expectedS3Bucket: String): Unit = {
    val u = container.jdbcUrl
    
    val jdbcUrl = new URI(if(u.startsWith("jdbc:")) u.drop("jdbc:".size) else u)
    
    val expectedContents = s"""|INTAKE_S3_BUCKET=${expectedS3Bucket}
                               |INTAKE_DB_HOST=${jdbcUrl.getHost}
                               |INTAKE_DB_PORT=${jdbcUrl.getPort}
                               |INTAKE_DB_USER=${container.username}
                               |INTAKE_DB_PASSWORD=${container.password}
                               |INTAKE_DB_NAME=${container.databaseName}""".stripMargin
                               
    assert(Files.readFrom(envFile) === expectedContents)
  }
  
  private def deleteUploadedData(metadata: aggregator.Metadata): Unit = {
    val uploadDir = s"variants/${metadata.dataset}"
    
    aws.rmdir(uploadDir).unsafeRunSync()
    
    import org.broadinstitute.dig.aws.Implicits._ 
    
    assert(aws.s3.keyExists(s3Bucket, uploadDir) === false)
  }
  
  private def getUploadedData(metadata: aggregator.Metadata): String = {
    val uploadDir = s"variants/${metadata.dataset}/${metadata.phenotype}"
    
    val expectedPartFile = s"${uploadDir}/part-1"
    
    val uploadedKeys = aws.ls(uploadDir).unsafeRunSync()
    
    assert(uploadedKeys === Seq(expectedPartFile))
    
    import org.broadinstitute.dig.aws.Implicits._
    
    (for {
      response <- aws.get(expectedPartFile)
    } yield response.readAsString()).unsafeRunSync()
  }
  
  private def expectedDataAsRows: Seq[CsvRow] = {
    CsvSource.FastCsv.fromReader(new StringReader(expectedMungedContents)).records.toIndexedSeq
  }
  
  private def uploadedDataAsRows(data: String): Seq[CsvRow] = {
    import org.json4s._
    import org.json4s.jackson._
    import org.json4s.jackson.Serialization.{read,write}
    
    def toJObject(s: String): JObject = parseJson(s).asInstanceOf[JObject]
    
    def toCsvRow(jobject: JObject): CsvRow = {
      implicit val formats = DefaultFormats
      
      def stripQuotes(s: String): String = {
        val withoutLeadingQuote = if(s.head == '\"') s.drop(1) else s
        
        if(withoutLeadingQuote.last == '\"') withoutLeadingQuote.dropRight(1) else withoutLeadingQuote 
      }
      
      new CsvRow {
        override def getFieldByName(name: String): String = stripQuotes(write(jobject \ name))
    
        override def getFieldByIndex(i: Int): String = ???
      }
    }
    
    val lines = data.split(System.lineSeparator)
    
    lines.map(toJObject).map(toCsvRow)
  }
  
  private val refAndAltRegex = """^.+?(.)\:(.)$""".r 
    
  private def refAndAlt(uploadedVarId: String): (String, String) = uploadedVarId match {
    case refAndAltRegex(ref, alt) => (ref, alt)
    case _ => ???
  }
  
  private def mapBy(rows: Seq[CsvRow], fieldName: String): Map[String, CsvRow] = {
    import Traversables.Implicits._
      
    rows.mapBy(_.getFieldByName(fieldName))
  }
  
  private def doAssertFieldsMatch(
      uploadedRowAndFieldName: (CsvRow, String), 
      expectedRowAndFieldName: (CsvRow, String)): Unit = {
    
    val (uploadedRow, uploadedFieldName) = uploadedRowAndFieldName
    val (expectedRow, expectedFieldName) = expectedRowAndFieldName
    
    assert(
        uploadedRow.getFieldByName(uploadedFieldName) == expectedRow.getFieldByName(expectedFieldName),
        s"'${uploadedFieldName}' didn't match '${expectedFieldName}'")
  }
  
  private def doAssertDoubleFieldsMatch(
      uploadedRowAndFieldName: (CsvRow, String), 
      expectedRowAndFieldName: (CsvRow, String)): Unit = {
    
    val (uploadedRow, uploadedFieldName) = uploadedRowAndFieldName
    val (expectedRow, expectedFieldName) = expectedRowAndFieldName
    
    val uploaded = uploadedRow.getFieldByName(uploadedFieldName).toDouble
    val expected = expectedRow.getFieldByName(expectedFieldName).toDouble
    
    val delta = 0.000000000000001
    
    //round-trip json marshalling and unmarshalling can introduce very small differences in floating-point values  
    assert(
        scala.math.abs(expected - uploaded) <= delta, 
        s"'${uploadedFieldName}' didn't match '${expectedFieldName}'")
  }
  
  private def validateUploadedData(metadata: aggregator.Metadata): Unit = {
    val uploadedData = getUploadedData(metadata)
    
    val expectedRowsByVarId: Map[String, CsvRow] = mapBy(expectedDataAsRows, "VAR_ID")
    
    val uploadedRowsByVarId: Map[String, CsvRow] = mapBy(uploadedDataAsRows(uploadedData), "varId")
    
    for {
      (varIdInUploadedFormat, uploadedRow) <- uploadedRowsByVarId
    } {
      val expectedRow = expectedRowsByVarId(varIdInUploadedFormat.replaceAll("\\:", "_"))
      
      def assertFieldsMatch(uploadedFieldName: String, expectedFieldName: String): Unit = {
        doAssertFieldsMatch(uploadedRow -> uploadedFieldName, expectedRow -> expectedFieldName)
      }
      
      def assertDoubleFieldsMatch(uploadedFieldName: String, expectedFieldName: String): Unit = {
        doAssertDoubleFieldsMatch(uploadedRow -> uploadedFieldName, expectedRow -> expectedFieldName)
      }
      
      assert(uploadedRow.getFieldByName("dataset") === metadata.dataset)
      assert(uploadedRow.getFieldByName("phenotype") === metadata.phenotype)
      assert(uploadedRow.getFieldByName("ancestry") === metadata.ancestry)
      assert(uploadedRow.getFieldByName("multiAllelic") === "false")
      
      val (ref, alt) = refAndAlt(varIdInUploadedFormat)
      
      assert(uploadedRow.getFieldByName("reference") === ref)
      assert(uploadedRow.getFieldByName("alt") === alt)
      
      assertFieldsMatch("chromosome", "CHROM")
      assertFieldsMatch("position", "POS")
      assertDoubleFieldsMatch("pValue", "P_VALUE")
      assertDoubleFieldsMatch("beta", "ODDS_RATIO")
      assertDoubleFieldsMatch("oddsRatio", "ODDS_RATIO")
      assertDoubleFieldsMatch("zScore", "ODDS_RATIO")
      assertDoubleFieldsMatch("eaf", "EAF")
      assertDoubleFieldsMatch("maf", "MAF")
      assertDoubleFieldsMatch("stdErr", "SE")
      
      assert(uploadedRow.getFieldByName("n").toInt === metadata.subjects)
    }
    
    assert(expectedRowsByVarId.size === uploadedRowsByVarId.size)
  }
}

object CsvTransformationTest {
  private object Loam {
    object ColumnNames {
      import IntakeSyntax._
      
      val VARID = "VAR_ID".asColumnName
      val CHROM = "CHROM".asColumnName
      val POS = "POS".asColumnName
      val Allele1 = "Allele1".asColumnName
      val Allele2 = "Allele2".asColumnName
      val ReferenceAllele = "Reference_Allele".asColumnName
      val EffectAllele = "Effect_Allele".asColumnName
      val EffectAllelePH = "Effect_Allele_PH".asColumnName
      val EAF = "EAF".asColumnName
      val EAFPH = "EAF_PH".asColumnName
      val MAF = "MAF".asColumnName
      val MAFPH = "MAF_PH".asColumnName
      val OddsRatio = "ODDS_RATIO".asColumnName
      val SE = "SE".asColumnName
      val PValue = "P_VALUE".asColumnName
      val Freq1 = "Freq1".asColumnName
      val Effect = "Effect".asColumnName
      val StdErr = "StdErr".asColumnName
      val PDashValue = "P-value".asColumnName
    }
  }
  
  private final class Paths(workDir: Path) {
    val mungedOutputPath: Path = workDir.resolve("munged.txt")
    val schemaFilePath: Path = workDir.resolve("schema.txt")
    val dataListFilePath: Path = workDir.resolve("data.list")
    val schemaListFilePath: Path = workDir.resolve("schema.list")
    val aggregatorConfigFilePath: Path = workDir.resolve("aggregator-intake.conf")
    val aggregatorEnvFilePath: Path = workDir.resolve("aggregator-intake.env")
  }
  
  private val tab = '\t'
      
  // scalastyle:off line.size.limit
  private val expectedMungedContents = { 
s"""VAR_ID${tab}CHROM${tab}POS${tab}Reference_Allele${tab}Effect_Allele${tab}Effect_Allele_PH${tab}EAF${tab}EAF_PH${tab}MAF${tab}MAF_PH${tab}ODDS_RATIO${tab}SE${tab}P_VALUE
11_100009976_G_A${tab}11${tab}100009976${tab}G${tab}A${tab}A${tab}0.869${tab}0.869${tab}0.131${tab}0.131${tab}1.0149100623037037${tab}0.0121${tab}0.2228
17_63422266_C_A${tab}17${tab}63422266${tab}C${tab}A${tab}A${tab}0.0305${tab}0.0305${tab}0.0305${tab}0.0305${tab}0.9856046187323824${tab}0.0244${tab}0.5517
10_29561930_C_T${tab}10${tab}29561930${tab}C${tab}T${tab}T${tab}0.9872${tab}0.9872${tab}0.012800000000000034${tab}0.012800000000000034${tab}0.9757001140283413${tab}0.0445${tab}0.58
18_44199217_G_A${tab}18${tab}44199217${tab}G${tab}A${tab}A${tab}0.6401${tab}0.6401${tab}0.3599${tab}0.3599${tab}1.0214263164736588${tab}0.0085${tab}0.01243
11_107492412_C_T${tab}11${tab}107492412${tab}C${tab}T${tab}T${tab}0.4075${tab}0.4075${tab}0.4075${tab}0.4075${tab}1.0174505116980552${tab}0.01${tab}0.08484
7_79482738_T_A${tab}7${tab}79482738${tab}T${tab}A${tab}A${tab}0.2998${tab}0.2998${tab}0.2998${tab}0.2998${tab}0.9901488436829572${tab}0.0089${tab}0.2676
16_81620003_C_T${tab}16${tab}81620003${tab}C${tab}T${tab}T${tab}0.063${tab}0.063${tab}0.063${tab}0.063${tab}1.003104809969017${tab}0.0171${tab}0.8579
3_143120904_C_T${tab}3${tab}143120904${tab}C${tab}T${tab}T${tab}0.9652${tab}0.9652${tab}0.03480000000000005${tab}0.03480000000000005${tab}1.0010005001667084${tab}0.0227${tab}0.9663
2_164332357_C_T${tab}2${tab}164332357${tab}C${tab}T${tab}T${tab}0.3383${tab}0.3383${tab}0.3383${tab}0.3383${tab}0.987874118279475${tab}0.0086${tab}0.1577
"""
  // scalastyle:on line.size.limit
  }

  private val expectedSchemaFileContents = {
s"""VAR_ID${tab}STRING
CHROM${tab}STRING
POS${tab}STRING
Reference_Allele${tab}STRING
Effect_Allele${tab}STRING
Effect_Allele_PH${tab}STRING
EAF${tab}FLOAT
EAF_PH${tab}FLOAT
MAF${tab}FLOAT
MAF_PH${tab}FLOAT
ODDS_RATIO${tab}FLOAT
SE${tab}FLOAT
P_VALUE${tab}FLOAT"""        
      }
      
  private def expectedConfigFileContents(mungedOutputPath: Path) = {
s"""dataset some-dataset some-phenotype
ancestry some-ancestry
tech some-tech
cases 42
controls 21
subjects 63
var_id {chrom}_{pos}_{ref}_{alt}
author some-author

marker VAR_ID
pvalue P_VALUE
zscore ODDS_RATIO
stderr SE
beta ODDS_RATIO
odds_ratio ODDS_RATIO
eaf EAF
maf MAF

load ${mungedOutputPath}

process

quit"""
  }
}
