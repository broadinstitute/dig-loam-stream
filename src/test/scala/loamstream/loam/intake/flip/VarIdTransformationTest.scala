package loamstream.loam.intake.flip

import org.scalatest.FunSuite

import loamstream.loam.intake.CsvRow
import loamstream.loam.intake.LiteralColumnExpr
import loamstream.loam.intake.IntakeSyntax



/**
 * @author clint
 * Apr 3, 2020
 */
final class VarIdTransformationTest extends FunSuite {
  
  private lazy val flipDetector: FlipDetector = FlipDetectorTest.makeFlipDetector
  
  import VarIdTransformationTest.LiteralCsvRow
  import IntakeSyntax._
  
  private val varIdColumnName = ColumnName("VAR_ID")
  private val pvalueColumnName = ColumnName("P_VALUE")
  
  private val variantExpr = varIdColumnName.map(Variant.from)
    
  private val varIdDef = MarkerColumnDef(
    varIdColumnName,
    variantExpr)
    
  private val toAggregatorRow: AggregatorRowExpr = AggregatorRowExpr(
    markerDef = varIdDef,
    pvalueDef = NamedColumnDef(AggregatorColumnNames.pvalue, LiteralColumnExpr(42.0)))
  
  
  test("problematic variant 1_713131_AT_A") {

    val v = Variant.from("1_713131_AT_A")
    
    val rows = Source.fromString(s"""|${varIdColumnName.name}
                                     |${v.underscoreDelimited}""".stripMargin)
    
    val dataRows = rows.tagFlips(varIdDef, flipDetector).map(toAggregatorRow).records.toIndexedSeq
    
    assert(dataRows.size === 1)
    assert(dataRows.head._2.marker === v)
  }
  
  test("Var ids are transformed properly when flips are detected") {
    import IntakeSyntax._
    
    def inputVarIds = inputsAndExpectedOutputs.iterator.collect { case (i, _) => i }
    
    val source: Source[CsvRow] = Source.FromIterator {
      inputVarIds.map(i => LiteralCsvRow(varIdColumnName.name, i.underscoreDelimited)) 
    }
    
    val dataRows = source.tagFlips(varIdDef, flipDetector).map(toAggregatorRow)
      
    val actualVarIds = dataRows.map(_._2.marker)
      
    actualVarIds.records.zip(inputsAndExpectedOutputs.iterator).foreach { case (actual, (input, expected)) =>
      def msg = {
        s"Input '$input' should have been turned into '$expected' but got '$actual'. " + 
        s"Input flipped? ${flipDetector.isFlipped(input)}"
      }
      
      assert(actual == expected, msg)
    }
  }
  
  import loamstream.loam.intake.Variant
  
  private val inputsAndExpectedOutputs: Seq[(Variant, Variant)] = Seq(
    ("1_612688_T_TCTC","1_612688_T_TCTC"),
    ("1_636285_C_T","1_636285_T_C"),
    ("1_649192_T_A","1_649192_A_T"),
    ("1_662414_T_C","1_662414_C_T"),
    ("1_662622_A_G","1_662622_G_A"),
    ("1_665266_C_T","1_665266_T_C"),
    ("1_692794_C_CA","1_692794_C_CA"),
    ("1_693731_G_A","1_693731_A_G"),
    ("1_693823_C_G","1_693823_G_C"),
    ("1_701835_C_T","1_701835_T_C"),
    ("1_705882_A_G","1_705882_G_A"),
    ("1_706368_G_A","1_706368_A_G"),
    ("1_706778_A_G","1_706778_G_A"),
    ("1_707522_C_G","1_707522_G_C"),
    ("1_711310_A_G","1_711310_G_A"),
    ("1_712547_C_G","1_712547_G_C"),
    ("1_712762_G_T","1_712762_T_G"),
    ("1_713131_AT_A","1_713131_AT_A"),
    ("1_713914_G_A","1_713914_A_G"),
    ("1_714019_G_A","1_714019_A_G"),
    ("1_714310_G_C","1_714310_C_G"),
    ("1_714427_A_G","1_714427_G_A"),
    ("1_714596_C_T","1_714596_T_C"),
    ("1_715265_T_C","1_715265_C_T"),
    ("1_715367_G_A","1_715367_A_G"),
    ("1_717485_A_C","1_717485_C_A"),
    ("1_717587_A_G","1_717587_G_A"),
    ("1_719854_C_CAG","1_719854_C_CAG"),
    ("1_719914_G_C","1_719914_C_G"),
    ("1_720381_T_G","1_720381_G_T"),
    ("1_721290_C_G","1_721290_G_C"),
    ("1_721757_A_T","1_721757_T_A"),
    ("1_722364_G_GATTT","1_722364_G_GATTT"),
    ("1_722670_C_T","1_722670_T_C"),
    ("1_723307_G_C","1_723307_C_G"),
    ("1_723742_C_T","1_723742_T_C"),
    ("1_723753_A_AGAGAGAGG","1_723753_A_AGAGAGAGG"),
    ("1_723798_C_CAG","1_723798_C_CAG"),
    ("1_723819_A_T","1_723819_T_A"),
    ("1_723891_C_G","1_723891_G_C"),
    ("1_724295_T_TGGAAC","1_724295_T_TGGAAC"),
    ("1_724324_A_G","1_724324_G_A"),
    ("1_724621_C_CAAATG","1_724621_C_CAAATG"),
    ("1_725196_A_G","1_725196_G_A"),
    ("1_725322_G_GAATGGAATGGAATGC","1_725322_G_GAATGGAATGGAATGC"),
    ("1_725389_T_C","1_725389_C_T"),
    ("1_725401_T_C","1_725401_C_T"),
    ("1_725438_A_AATGGGATGGGATGGGATGGGATGCG","1_725438_A_AATGGGATGGGATGGGATGGGATGCG"),
    ("1_726455_TGGAAG_T","1_726455_TGGAAG_T"),
    ("1_726794_G_C","1_726794_C_G"),
    ("1_727286_A_AAG","1_727286_A_AAG"),
    ("1_727841_A_G","1_727841_G_A"),
    ("1_729632_T_C","1_729632_C_T"),
    ("1_729679_G_C","1_729679_C_G"),
    ("1_730087_C_T","1_730087_T_C"),
    ("1_731718_C_T","1_731718_T_C"),
    ("1_732032_C_A","1_732032_A_C"),
    ("1_732215_T_C","1_732215_C_T"),
    ("1_732809_C_T","1_732809_T_C"),
    ("1_732989_T_C","1_732989_C_T"),
    ("1_733235_G_T","1_733235_T_G"),
    ("1_734349_C_T","1_734349_T_C"),
    ("1_735985_A_G","1_735985_G_A"),
    ("1_736289_A_T","1_736289_T_A"),
    ("1_736689_C_T","1_736689_T_C"),
    ("1_736736_G_A","1_736736_A_G"),
    ("1_738475_A_G","1_738475_G_A"),
    ("1_738965_A_G","1_738965_G_A"),
    ("1_739117_A_G","1_739117_G_A"),
    ("1_739528_A_G","1_739528_G_A"),
    ("1_740284_T_C","1_740284_C_T"),
    ("1_740285_A_G","1_740285_G_A"),
    ("1_741397_G_A","1_741397_A_G"),
    ("1_742990_T_C","1_742990_C_T"),
    ("1_743420_A_G","1_743420_G_A"),
    ("1_745021_T_G","1_745021_G_T"),
    ("1_745642_A_AC","1_745642_A_AC"),
    ("1_746211_AG_A","1_746211_A_AG"),
    ("1_746727_A_G","1_746727_G_A"),
    ("1_747753_T_TGC","1_747753_T_TGC"),
    ("1_747966_A_G","1_747966_G_A"),
    ("1_748141_A_G","1_748141_G_A"),
    ("1_748279_T_A","1_748279_A_T"),
    ("1_748765_A_G","1_748765_G_A"),
    ("1_748878_T_G","1_748878_G_T"),
    ("1_749963_TAA_T","1_749963_TAA_T"),
    ("1_750055_C_T","1_750055_T_C"),
    ("1_751343_A_T","1_751343_T_A"),
    ("1_751488_GA_G","1_751488_GA_G"),
    ("1_751756_C_T","1_751756_T_C"),
    ("1_752307_A_AT","1_752307_A_AT"),
    ("1_752478_A_G","1_752478_G_A"),
    ("1_752566_A_G","1_752566_G_A"),
    ("1_752593_G_T","1_752593_T_G"),
    ("1_752617_A_C","1_752617_C_A"),
    ("1_752721_G_A","1_752721_A_G"),
    ("1_752894_C_T","1_752894_T_C"),
    ("1_753405_A_C","1_753405_C_A"),
    ("1_753425_C_T","1_753425_T_C")).map { case (i, e) => (Variant.from(i), Variant.from(e)) }
}

object VarIdTransformationTest {
  final case class LiteralCsvRow(private val fieldName: String, private val fieldValue: String) extends CsvRow {
    override def getFieldByName(name: String): String = {
      require(name == fieldName)
      
      fieldValue
    }
  
    override def getFieldByIndex(i: Int): String = {
      require(i == 0)
      
      fieldValue
    }
    
    override def size: Int = 1
    
    override def recordNumber: Long = ???
  }
}
