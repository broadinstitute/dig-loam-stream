package loamstream.loam.intake.flip

import org.scalatest.FunSuite
import java.nio.file.Paths
import loamstream.loam.intake.Variant
import loamstream.util.TimeUtils

/**
 * @author clint
 * Apr 2, 2020
 */
final class FlipDetectorTest extends FunSuite {
  private lazy val flipDetector: FlipDetector = FlipDetectorTest.makeFlipDetector

  import Disposition._
  
  test("complement+flip case") {
    assert(flipDetector.isFlipped(Variant.from("1_636285_G_A")) === Disposition.FlippedComplementStrand)
  }
  
  test("complement case") {
    assert(flipDetector.isFlipped(Variant.from("1_636285_A_G")) === Disposition.NotFlippedComplementStrand)
  }
  
  test("Problematic variant: 1_636285_T_C") {
    assert(flipDetector.isFlipped(Variant.from("1_636285_T_C")) === Disposition.NotFlippedSameStrand)
    assert(flipDetector.isFlipped(Variant.from("1_636285_C_T")) === Disposition.FlippedSameStrand)
  }
  
  test("Problematic variant: 1_649192_T_A") {
    assert(flipDetector.isFlipped(Variant.from("1_649192_T_A")) === Disposition.FlippedSameStrand)
    assert(flipDetector.isFlipped(Variant.from("1_649192_A_T")) === Disposition.NotFlippedSameStrand)
  }
 
  test("Problematic variant: 1_612688_T_TCTC") {
    assert(flipDetector.isFlipped(Variant.from("1_612688_T_TCTC")) === Disposition.NotFlippedSameStrand)
    assert(flipDetector.isFlipped(Variant.from("1_612688_TCTC_T")) === Disposition.FlippedSameStrand)
  }
  
  test("Problematic variant: 1_746211_AG_A") {
    assert(flipDetector.isFlipped(Variant.from("1_746211_AG_A")) === Disposition.FlippedSameStrand)
    assert(flipDetector.isFlipped(Variant.from("1_746211_A_AG")) === Disposition.NotFlippedSameStrand)
  }
  
  test("Problematic variant: 1_738475_A_G") {
    assert(flipDetector.isFlipped(Variant.from("1_738475_A_G")) === Disposition.FlippedSameStrand)
    assert(flipDetector.isFlipped(Variant.from("1_738475_G_A")) === Disposition.NotFlippedSameStrand)
  }
  
  test("Problematic variants") {
    def doTest(variant: String): Unit = {
      val v = Variant.from(variant)
      
      assert(
          flipDetector.isFlipped(v) === Disposition.FlippedSameStrand, 
          s"Expected ${v.underscoreDelimited} to be flipped")
          
      assert(
          flipDetector.isFlipped(v.flip) === Disposition.NotFlippedSameStrand, 
          s"Expected ${v.underscoreDelimited} to NOT be flipped")
    }
    
    problematicVariants.foreach(doTest)
  }
  
  private val problematicVariants: Seq[String] = Seq(
    "1_636285_C_T",
    "1_649192_T_A",
    "1_662414_T_C",
    "1_662622_A_G",
    "1_665266_C_T",
    "1_693731_G_A",
    "1_693823_C_G",
    "1_701835_C_T",
    "1_705882_A_G",
    "1_706368_G_A",
    "1_706778_A_G",
    "1_707522_C_G",
    "1_711310_A_G",
    "1_712547_C_G",
    "1_712762_G_T",
    "1_713914_G_A",
    "1_714019_G_A",
    "1_714310_G_C",
    "1_714427_A_G",
    "1_714596_C_T",
    "1_715265_T_C",
    "1_715367_G_A",
    "1_717485_A_C",
    "1_717587_A_G",
    "1_719914_G_C",
    "1_720381_T_G",
    "1_721290_C_G",
    "1_721757_A_T",
    "1_722670_C_T",
    "1_723307_G_C",
    "1_723742_C_T",
    "1_723819_A_T",
    "1_723891_C_G",
    "1_724324_A_G",
    "1_725196_A_G",
    "1_725389_T_C",
    "1_725401_T_C",
    "1_726794_G_C",
    "1_727841_A_G",
    "1_729632_T_C",
    "1_729679_G_C",
    "1_730087_C_T",
    "1_731718_C_T",
    "1_732032_C_A",
    "1_732215_T_C",
    "1_732809_C_T",
    "1_732989_T_C",
    "1_733235_G_T",
    "1_734349_C_T",
    "1_735985_A_G",
    "1_736289_A_T",
    "1_736689_C_T",
    "1_736736_G_A",
    "1_738475_A_G",
    "1_738965_A_G",
    "1_739117_A_G",
    "1_739528_A_G",
    "1_740284_T_C",
    "1_740285_A_G",
    "1_741397_G_A",
    "1_742990_T_C",
    "1_743420_A_G",
    "1_745021_T_G",
    "1_746211_AG_A",
    "1_746727_A_G",
    "1_747966_A_G",
    "1_748141_A_G",
    "1_748279_T_A",
    "1_748765_A_G",
    "1_748878_T_G",
    "1_750055_C_T",
    "1_751343_A_T",
    "1_751756_C_T",
    "1_752478_A_G",
    "1_752566_A_G",
    "1_752593_G_T",
    "1_752617_A_C",
    "1_752721_G_A",
    "1_752894_C_T",
    "1_753405_A_C",
    "1_753425_C_T")
}

object FlipDetectorTest {
  private[flip] def makeFlipDetector: FlipDetector = {
    new FlipDetector.Default(
      referenceDir = Paths.get("src/test/resources/intake/reference-first-1M-of-chrom1/"),
      isVarDataType = true,
      pathTo26kMap = Paths.get("src/test/resources/intake/26k_id.map.first100"))
  }
}
