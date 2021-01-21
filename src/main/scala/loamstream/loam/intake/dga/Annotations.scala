package loamstream.loam.intake.dga

import org.json4s._
import scala.util.Try
import loamstream.util.LogContext
import loamstream.util.Tries

/**
 * @author clint
 * Jan 20, 2021
 * 
 * All annotations available from DGA broken up by their respective terms.
 * 
 * @see Annotation in processors/vendors/dga/download.py
 */
final case class Annotations private[dga] (
    //GREGOR REGIONS
    accessible_chromatin: Iterable[Annotation],
    chromatin_state: Iterable[Annotation],
    binding_sites: Iterable[Annotation],
    eQTL: Iterable[Annotation],
    //PAIRED BED FILES
    coaccessible_target_genes: Iterable[Annotation],
    chromatin_interaction_target_genes: Iterable[Annotation],
    //OTHER...
    gene_expression: Iterable[Annotation],
    variant_allelic_effects: Iterable[Annotation],
    promoter_like_regions: Iterable[Annotation]
    ){
    //def __init__(self, assembly, **fields):
        /*accessible_chromatin = fields.get('accessible chromatin', [])
        gene_expressions = fields.get('gene expression', [])
        chromatin_states = fields.get('chromatin state', [])
        binding_sites = fields.get('binding sites', [])
        variant_allelic_effects = fields.get('variant allelic effects', [])
        promoter_like_regions = fields.get('promoter-like regions', [])
        coaccessible_target_genes = fields.get('Coaccessible target genes', [])
        chromatin_interaction_target_genes = fields.get('Chromatin interaction target genes', [])
        eQTL = fields.get('eQTL', [])*/

        /*# GREGOR REGIONS
        self.accessible_chromatin = [Annotation(assembly, **x) for x in accessible_chromatin]
        self.chromatin_state = [Annotation(assembly, **x) for x in chromatin_states]
        self.binding_sites = [Annotation(assembly, **x) for x in binding_sites]
        self.eQTL = [Annotation(assembly, **x) for x in eQTL]

        # PAIRED BED FILES
        self.coaccessible_target_genes = [Annotation(assembly, **x) for x in coaccessible_target_genes]
        self.chromatin_interaction_target_genes = [Annotation(assembly, **x) for x in chromatin_interaction_target_genes]

        # OTHER...
        self.gene_expression = [Annotation(assembly, **x) for x in gene_expressions]
        self.variant_allelic_effects = [Annotation(assembly, **x) for x in variant_allelic_effects]
        self.promoter_like_regions = [Annotation(assembly, **x) for x in promoter_like_regions]*/
}

object Annotations {
  import Json.JsonOps
  
  def fromJson(assemblyId: String)(json: JValue)(implicit ctx: LogContext): Try[Annotations] = {
    def toAnnotationIterable(fieldName: String): Try[Iterable[Annotation]] = {
      val jvs = json.tryAsArray(fieldName).getOrElse(Nil)
      
      Tries.sequence(jvs.map(Annotation.fromJson(assemblyId)))
    }
    
    for {
      accessible_chromatin <- toAnnotationIterable("accessible chromatin")
      gene_expression <- toAnnotationIterable("gene expression")
      chromatin_state <- toAnnotationIterable("chromatin state")
      binding_sites <- toAnnotationIterable("binding sites")
      variant_allelic_effects <- toAnnotationIterable("variant allelic effects")
      promoter_like_regions <- toAnnotationIterable("promoter-like regions")
      coaccessible_target_genes <- toAnnotationIterable("Coaccessible target genes")
      chromatin_interaction_target_genes <- toAnnotationIterable("Chromatin interaction target genes")
      eQTL <- toAnnotationIterable("eQTL")
    } yield {
      Annotations(
        accessible_chromatin = accessible_chromatin,
        chromatin_state = chromatin_state,
        binding_sites = binding_sites,
        eQTL = eQTL,
        coaccessible_target_genes = coaccessible_target_genes,
        chromatin_interaction_target_genes = chromatin_interaction_target_genes,
        gene_expression = gene_expression,
        variant_allelic_effects = variant_allelic_effects,
        promoter_like_regions = promoter_like_regions)
    }
  }
}
