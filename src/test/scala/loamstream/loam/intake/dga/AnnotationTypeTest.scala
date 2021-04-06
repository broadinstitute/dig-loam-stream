package loamstream.loam.intake.dga

import org.scalatest.FunSuite
import scala.util.Success

/**
 * @author clint
 * Feb 24, 2021
 */
final class AnnotationTypeTest extends FunSuite {
  import AnnotationType._
  
  test("name") {
    assert(AccessibleChromatin.name === "accessible_chromatin")
    assert(ChromatinState.name === "chromatin_state")
    assert(BindingSites.name === "binding_sites")
    assert(TargetGenePrediction.name === "target_gene_prediction")
    assert(CandidateRegulatoryRegions.name === "candidate_regulatory_regions")
    assert(HistoneModifications.name === "histone_modifications")
    assert(VariantAllelicEffects.name === "variant_allelic_effects")
    assert(EQTL.name === "eqtl")
    assert(CaQTL.name === "caqtl")
    assert(BindingFootprint.name === "binding_footprint")
  }
  
  test("values") {
    val expected: Set[AnnotationType] = Set(
      AccessibleChromatin, 
      ChromatinState, 
      BindingSites, 
      TargetGenePrediction,
      CandidateRegulatoryRegions,
      HistoneModifications,
      VariantAllelicEffects,
      EQTL,
      CaQTL,
      BindingFootprint)
      
    assert(AnnotationType.values === expected)
  }
  
  test("fromString") {
    assert(fromString("accessible_chromatin") === Some(AccessibleChromatin))
    assert(fromString("chromatin_state") === Some(ChromatinState))
    assert(fromString("binding_sites") === Some(BindingSites))
    assert(fromString("target_gene_prediction") === Some(TargetGenePrediction))
    assert(fromString("candidate_regulatory_regions") === Some(CandidateRegulatoryRegions))
    assert(fromString("histone_modifications") === Some(HistoneModifications))
    assert(fromString("variant_allelic_effects") === Some(VariantAllelicEffects))
    assert(fromString("eQTL") === Some(EQTL))
    assert(fromString("caQTL") === Some(CaQTL))
    assert(fromString("binding_footprint") === Some(BindingFootprint))
    
    assert(fromString("Accessible chroMatin") === Some(AccessibleChromatin))
    assert(fromString("Chromatin StAte") === Some(ChromatinState))
    assert(fromString("Binding SiTes") === Some(BindingSites))
    assert(fromString("Target Gene PrediCtion") === Some(TargetGenePrediction))
    assert(fromString("CandidatE reguLatory Regions") === Some(CandidateRegulatoryRegions))
    assert(fromString("HistonE Modifications") === Some(HistoneModifications))
    assert(fromString("Variant alleliC effects") === Some(VariantAllelicEffects))
    assert(fromString("EQTL") === Some(EQTL))
    assert(fromString("CAqtl") === Some(CaQTL))
    assert(fromString("BInding FOotprint") === Some(BindingFootprint))
    
    assert(fromString("") === None)
    assert(fromString("asfsadgfsd") === None)
    assert(fromString("   ") === None)
  }
  
  test("tryFromString") {
    assert(tryFromString("accessible_chromatin") === Success(AccessibleChromatin))
    assert(tryFromString("chromatin_state") === Success(ChromatinState))
    assert(tryFromString("binding_sites") === Success(BindingSites))
    assert(tryFromString("target_gene_prediction") === Success(TargetGenePrediction))
    assert(tryFromString("candidate_regulatory_regions") === Success(CandidateRegulatoryRegions))
    assert(tryFromString("histone_modifications") === Success(HistoneModifications))
    assert(tryFromString("variant_allelic_effects") === Success(VariantAllelicEffects))
    assert(tryFromString("eQTL") === Success(EQTL))
    assert(tryFromString("caQTL") === Success(CaQTL))
    assert(tryFromString("binding_footprint") === Success(BindingFootprint))
    
    assert(tryFromString("Accessible chroMatin") === Success(AccessibleChromatin))
    assert(tryFromString("Chromatin StAte") === Success(ChromatinState))
    assert(tryFromString("Binding SiTes") === Success(BindingSites))
    assert(tryFromString("Target Gene PrediCtion") === Success(TargetGenePrediction))
    assert(tryFromString("CandidatE reguLatory Regions") === Success(CandidateRegulatoryRegions))
    assert(tryFromString("HistonE Modifications") === Success(HistoneModifications))
    assert(tryFromString("Variant alleliC effects") === Success(VariantAllelicEffects))
    assert(tryFromString("EQTL") === Success(EQTL))
    assert(tryFromString("CAqtl") === Success(CaQTL))
    assert(tryFromString("BInding FOotprint") === Success(BindingFootprint))
    
    assert(tryFromString("").isFailure)
    assert(tryFromString("asfsadgfsd").isFailure)
    assert(tryFromString("   ").isFailure)
  }
}
