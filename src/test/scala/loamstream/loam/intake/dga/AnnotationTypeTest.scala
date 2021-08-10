package loamstream.loam.intake.dga

import org.scalatest.FunSuite
import scala.util.Success
import loamstream.TestHelpers
import scala.util.Try

/**
 * @author clint
 * Feb 24, 2021
 */
final class AnnotationTypeTest extends FunSuite {
  import AnnotationType._
  
  test("name") {
    assert(AccessibleChromatin.name === "accessible_chromatin")
    assert(BindingSites.name === "binding_sites")
    assert(CandidateRegulatoryElements.name === "candidate_regulatory_elements")
    assert(CaQTL.name === "caqtl")
    assert(ChromatinState.name === "chromatin_state")
    assert(EQTL.name === "eqtl")
    assert(GeneExpression.name === "gene_expression")
    assert(HistoneModifications.name === "histone_modifications")
    assert(HQTL.name === "hQTL")
    assert(MeQTL.name === "meQTL")
    assert(Other.name === "other")
    assert(RepresentativeDNaseHypersensitivitySites.name === "representative_DNase_hypersensitivity_sites") 
    assert(SQTL.name === "sQTL")
    assert(TargetGenePredictions.name === "target_gene_predictions")
    assert(VariantAllelicEffects.name === "variant_allelic_effects")
  }
  
  test("values") {
    val expected: Set[AnnotationType] = Set(
      AccessibleChromatin,
      BindingSites,
      CandidateRegulatoryElements,
      CaQTL,
      ChromatinState,
      EQTL,
      GeneExpression,
      HistoneModifications,
      HQTL,
      MeQTL,
      Other,
      RepresentativeDNaseHypersensitivitySites, 
      SQTL,
      TargetGenePredictions,
      VariantAllelicEffects)
      
    assert(AnnotationType.values === expected)
  }
  
  test("fromString") {
    def doTest(s: String, expected: Option[AnnotationType]): Unit = {
      val withSpaces = s.replaceAll("_", " ")
      
      assert(fromString(s) === expected)
      assert(fromString(withSpaces) === expected)
      assert(fromString(TestHelpers.to1337Speak(s)) === expected)
      assert(fromString(TestHelpers.to1337Speak(withSpaces)) === expected)
    }
    
    doTest("accessible_chromatin", Some(AccessibleChromatin))
    doTest("binding_sites", Some(BindingSites))
    doTest("candidate_regulatory_elements", Some(CandidateRegulatoryElements))
    doTest("caqtl", Some(CaQTL))
    doTest("chromatin_state", Some(ChromatinState))
    doTest("eqtl", Some(EQTL))
    doTest("gene_expression", Some(GeneExpression))
    doTest("histone_modifications", Some(HistoneModifications))
    doTest("hQTL", Some(HQTL))
    doTest("meQTL", Some(MeQTL))
    doTest("other", Some(Other))
    doTest("representative_DNase_hypersensitivity_sites", Some(RepresentativeDNaseHypersensitivitySites))
    doTest("sQTL", Some(SQTL))
    doTest("target_gene_predictions", Some(TargetGenePredictions))
    doTest("variant_allelic_effects", Some(VariantAllelicEffects))
    
    assert(fromString("") === None)
    assert(fromString("asfsadgfsd") === None)
    assert(fromString("   ") === None)
  }
  
  test("tryFromString") {
    def doTest(s: String, expected: Try[AnnotationType]): Unit = {
      val withSpaces = s.replaceAll("_", " ")
      
      assert(tryFromString(s) === expected)
      assert(tryFromString(withSpaces) === expected)
      assert(tryFromString(TestHelpers.to1337Speak(s)) === expected)
      assert(tryFromString(TestHelpers.to1337Speak(withSpaces)) === expected)
    }
    
    doTest("accessible_chromatin", Success(AccessibleChromatin))
    doTest("binding_sites", Success(BindingSites))
    doTest("candidate_regulatory_elements", Success(CandidateRegulatoryElements))
    doTest("caqtl", Success(CaQTL))
    doTest("chromatin_state", Success(ChromatinState))
    doTest("eqtl", Success(EQTL))
    doTest("gene_expression", Success(GeneExpression))
    doTest("histone_modifications", Success(HistoneModifications))
    doTest("hQTL", Success(HQTL))
    doTest("meQTL", Success(MeQTL))
    doTest("other", Success(Other))
    doTest("representative_DNase_hypersensitivity_sites", Success(RepresentativeDNaseHypersensitivitySites))
    doTest("sQTL", Success(SQTL))
    doTest("target_gene_predictions", Success(TargetGenePredictions))
    doTest("variant_allelic_effects", Success(VariantAllelicEffects))
    
    assert(tryFromString("").isFailure)
    assert(tryFromString("asfsadgfsd").isFailure)
    assert(tryFromString("   ").isFailure)
  }
}
