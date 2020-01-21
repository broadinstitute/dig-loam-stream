package loamstream.loam.intake

import org.scalatest.FunSuite
import loamstream.TestHelpers
import loamstream.loam.LoamSyntax
import loamstream.loam.intake.CsvSource.FastCsv.FromCommand
import loamstream.model.execute.RxExecuter
import loamstream.compiler.LoamEngine
import loamstream.util.Files

/**
 * @author clint
 * Dec 20, 2019
 */
final class CsvTransformationTest extends FunSuite {
  test("End-to-end CSV munging") {
    TestHelpers.withWorkDir(getClass.getSimpleName) { workDir =>
      val mungedOutputPath = workDir.resolve("munged.txt")
      val schemaFilePath = workDir.resolve("schema.txt")
      val dataListFilePath = workDir.resolve("data.list")
      val schemaListFilePath = workDir.resolve("schema.list")
      
      val graph = TestHelpers.makeGraph { implicit scriptContext =>
        import LoamSyntax._
        import IntakeSyntax._
        
        val storeA = store(path("src/test/resources/intake-data.txt")).asInput
        
        val storeB = store(workDir / "data.txt")
        
        cmd"cp $storeA $storeB".in(storeA).out(storeB)
        
        val storeC = store(mungedOutputPath)
        val storeSchema = store(schemaFilePath)
        val storeDataList = store(dataListFilePath)
        val storeSchemaList = store(schemaListFilePath)
        
        object ColumnNames {
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
        
        val columns: Seq[UnsourcedColumnDef] = {
          import ColumnNames._
          
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
        
        val source: CsvSource = FromCommand(s"cat ${storeB.path}")
        
        val flipDetector = new FlipDetector(
          referenceDir = path("/home/clint/workspace/marcins-scripts/reference"),
          isVarDataType = true,
          pathTo26kMap = path("/home/clint/workspace/marcins-scripts/26k_id.map"))
        
        val varIdColumn = columns.head.from(source)
        val otherColumns = source.producing(columns.tail)
        
        produceCsv(storeC).from(varIdColumn, otherColumns: _*).using(flipDetector).tag("makeCSV").in(storeB)
        
        produceSchemaFile(storeSchema).from(columns: _*).tag("makeSchemaFile")
        
        produceListFiles(storeDataList, storeSchemaList).from(storeC, storeSchema).tag("makeListFiles")
      }
      
      val executer = RxExecuter.default
      val executable = LoamEngine.toExecutable(graph)
      
      val results = executer.execute(executable)
      
      assert(results.size === 4)
      assert(results.values.forall(_.isSuccess))
          
      val tab = '\t'
      
      val expectedMungedContents = { 
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
      }

      val expectedSchemaFileContents = {
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
      
      assert(Files.readFrom(mungedOutputPath) === expectedMungedContents)
      
      assert(Files.readFrom(schemaFilePath) === expectedSchemaFileContents)
      
      assert(Files.readFrom(dataListFilePath) === s"${mungedOutputPath.toString}${System.lineSeparator}")
      assert(Files.readFrom(schemaListFilePath) === s"${schemaFilePath.toString}${System.lineSeparator}")
    }
  }
}
