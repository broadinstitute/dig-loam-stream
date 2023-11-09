package loamstream.loam.intake.dga

import loamstream.loam.intake.RenderableRow
import loamstream.loam.intake.RenderableJsonRow
import org.json4s.JsonAST.JValue

/**
 * @author clint
 * Jan 20, 2021
 */
final case class BedRow(
    dataset: String,
    biosampleId: Option[String],    // e.g. UBERON:293289
    biosampleType: Option[String],
    biosample: Option[String],
    tissueId: Option[String],   //TODO: Only Optional for now; e.g. UBERON:293289
    tissue: Option[String],
    annotation: String,    // annotation type, e.g. binding_site
    method: Option[String],  //TODO: Only Optional for now; e.g. MAC2
    source: Option[String],   //TODO: Only Optional for now; e.g. ATAC-seq-peak
    assay: Option[Seq[String]],   //TODO: Only Optional for now; e.g. ATAC-seq
    collection: Option[Seq[String]], //TODO: Only Optional for now; e.g. ENCODE
    chromosome: Option[String],
    start: Option[Long],
    end: Option[Long],
    state: Option[String], 
    targetGene: Option[String],    // only for annotation_type == "target_gene_predictions"
    targetGeneStart: Option[Long],    // only for annotation_type == "target_gene_predictions"
    targetGeneEnd: Option[Long],    // only for annotation_type == "target_gene_predictions"
    variant: Option[String],    // only for annotation_type == "variant_to_gene"
    gene: Option[String],    // only for annotation_type == "variant_to_gene"
    score: Option[Long],    // only for annotation_type == "variant_to_gene"
    info: Option[String],    // only for annotation_type == "variant_to_gene"
    ensemblId: Option[String],    // only for annotation_type == "gene_expression"
    nSamples: Option[String],    // only for annotation_type == "gene_expression"
    tpmForAllSamples: Option[String],    // only for annotation_type == "gene_expression"
    minTpm: Option[String],    // only for annotation_type == "gene_expression"
    firstQuTpm: Option[String],    // only for annotation_type == "gene_expression"
    medianTpm: Option[String],    // only for annotation_type == "gene_expression"
    meanTpm: Option[String],    // only for annotation_type == "gene_expression"
    thirdQuTpm: Option[String],    // only for annotation_type == "gene_expression"
    maxTpm: Option[String]    // only for annotation_type == "gene_expression"
  ) extends RenderableJsonRow {

  import Json.toJValue
  
  override def jsonValues: Seq[(String, JValue)] = {
    def noJNull[A](fieldName: String, opt: Option[A]): Iterator[(String, JValue)] = opt match {
      case Some(a) => Iterator(a).map(a => fieldName -> toJValue(a))
      case None => Iterator.empty
    }
    
    val tuples = Iterator(
      "dataset" -> toJValue(dataset),
      "biosampleId" -> toJValue(biosampleId),
      "biosampleType" -> toJValue(biosampleType),
      "biosample" -> toJValue(biosample),
      "tissueId" -> toJValue(tissueId),
      "tissue" -> toJValue(tissue),
      "annotation" -> toJValue(annotation),
      "method" -> toJValue(method),
      "source" -> toJValue(source),
      "assay" -> toJValue(assay),
      "collection" -> toJValue(collection)) ++
      noJNull("chromosome", chromosome) ++
      noJNull("start", start) ++
      noJNull("end", end) ++
      noJNull("targetGene", targetGene) ++
      noJNull("targetGeneStart", targetGeneStart) ++
      noJNull("targetGeneEnd", targetGeneEnd) ++
      noJNull("state", state) ++
      noJNull("variant", variant) ++
      noJNull("gene", gene) ++
      noJNull("score", score) ++
      noJNull("info", info) ++
      noJNull("ensemblId", ensemblId) ++
      noJNull("nSamples", nSamples) ++
      noJNull("tpmForAllSamples", tpmForAllSamples) ++
      noJNull("minTpm", minTpm) ++
      noJNull("firstQuTpm", firstQuTpm) ++
      noJNull("medianTpm", medianTpm) ++
      noJNull("meanTpm", meanTpm) ++
      noJNull("thirdQuTpm", thirdQuTpm) ++
      noJNull("maxTpm", maxTpm)
      
    tuples.toList
  }
}
