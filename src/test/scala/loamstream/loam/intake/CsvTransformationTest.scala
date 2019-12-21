package loamstream.loam.intake

import org.scalatest.FunSuite
import loamstream.TestHelpers
import loamstream.loam.LoamSyntax
import loamstream.loam.intake.CsvSource.FromCommand
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
      
      val graph = TestHelpers.makeGraph { implicit scriptContext =>
        import LoamSyntax._
        import IntakeSyntax._
        
        val storeA = store(path("src/test/resources/intake-data.txt")).asInput
        
        val storeB = store(workDir / "data.txt")
        
        cmd"cp $storeA $storeB".in(storeA).out(storeB)
        
        val storeC = store(mungedOutputPath)
        
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
            ColumnDef(EAF, Freq1, 1.0 - Freq1.asDouble),
            ColumnDef(EAFPH, Freq1, 1.0 - Freq1.asDouble),
            ColumnDef(MAF, Freq1.asDouble ~> { x => if(x > 0.5) 1.0 - x else x }),
            ColumnDef(MAFPH, Freq1.asDouble ~> { x => if(x > 0.5) 1.0 - x else x }),
            ColumnDef(
              OddsRatio, 
              (scala.math.exp _)(Effect.asDouble),//Effect.asDouble ~> scala.math.exp, 
              (-(Effect.asDouble)) ~> scala.math.exp),
            ColumnDef(SE, StdErr, StdErr),
            ColumnDef(PValue, PDashValue, PDashValue))
        }
        
        val source: CsvSource = CsvSource.FromCommand(s"cat ${storeB.path}")
        
        val tool = produceCsv(storeC).from(source.producing(columns): _*).in(storeB)
      }
      
      val executer = RxExecuter.default
      val executable = LoamEngine.toExecutable(graph)
      
      val results = executer.execute(executable)
      
      assert(results.size === 2)
      assert(results.values.forall(_.isSuccess))
          
      val tab = '\t'
      
      val expectedContents = { 
s"""VAR_ID${tab}CHROM${tab}POS${tab}Reference_Allele${tab}Effect_Allele${tab}Effect_Allele_PH${tab}EAF${tab}EAF_PH${tab}MAF${tab}MAF_PH${tab}ODDS_RATIO${tab}SE${tab}P_VALUE
11_100009976_G_A11${tab}100009976${tab}G${tab}A${tab}A${tab}0.8690${tab}0.8690${tab}0.131${tab}0.131${tab}1.0149100623037037${tab}${tab}${tab}0.0121${tab}0.2228
17_63422266_C_A${tab}17${tab}63422266${tab}C${tab}A${tab}A${tab}0.0305${tab}0.0305${tab}0.0305${tab}0.0305${tab}0.9856046187323824${tab}0.0244${tab}0.5517
10_29561930_C_T${tab}10${tab}29561930${tab}C${tab}T${tab}T${tab}0.9872${tab}0.9872${tab}0.012800000000000034${tab}0.012800000000000034${tab}0.9757001140283413${tab}0.0445${tab}0.58
18_44199217_G_A${tab}18${tab}44199217${tab}G${tab}A${tab}A${tab}0.6401${tab}0.6401${tab}0.3599${tab}0.3599${tab}1.0214263164736588${tab}0.0085${tab}0.01243
11_107492412_C_T${tab}11${tab}107492412${tab}C${tab}T${tab}T${tab}0.4075${tab}0.4075${tab}0.4075${tab}0.4075${tab}1.0174505116980552${tab}0.0100${tab}0.08484
7_79482738_T_A 7${tab}79482738${tab}T${tab}A${tab}A${tab}0.2998${tab}0.2998${tab}0.2998${tab}0.2998${tab}0.9901488436829572${tab}0.0089${tab}0.2676
16_81620003_C_T${tab}16${tab}81620003${tab}C${tab}T${tab}T${tab}0.0630${tab}0.0630${tab}0.063${tab}0.063${tab}1.003104809969017${tab}0.0171${tab}0.8579
3_143120904_C_T${tab}3${tab}143120904${tab}C${tab}T${tab}T${tab}0.9652${tab}0.9652${tab}0.03480000000000005${tab}0.03480000000000005${tab}1.0010005001667084${tab}0.0227${tab}0.9663
2_164332357_C_T${tab}2${tab}164332357${tab}C${tab}T${tab}T${tab}0.3383${tab}0.3383${tab}0.3383${tab}0.3383${tab}0.987874118279475${tab}0.0086${tab}0.1577
"""
      }
      
      assert(Files.readFrom(mungedOutputPath) === expectedContents)
    }
  }
}
