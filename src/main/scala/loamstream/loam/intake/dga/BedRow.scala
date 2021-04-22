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
    biosampleId: String,    // e.g. UBERON:293289
    biosampleType: String,
    biosample: Option[String],
    tissueId: Option[String],   //TODO: Only Optional for now; e.g. UBERON:293289
    tissue: Option[String],
    annotation: String,    // annotation type, e.g. binding_site
    category: Option[String], //TODO: Only Optional for now
    method: Option[String],  //TODO: Only Optional for now; e.g. MAC2
    source: Option[String],   //TODO: Only Optional for now; e.g. ATAC-seq-peak
    assay: Option[Seq[String]],   //TODO: Only Optional for now; e.g. ATAC-seq
    collection: Option[Seq[String]], //TODO: Only Optional for now; e.g. ENCODE
    chromosome: String,
    start: Long,
    end: Long,
    state: String, //was name
    targetGene: Option[String],    // only for annotation_type == "target_gene_prediction"
    targetGeneStart: Option[Long],    // only for annotation_type == "target_gene_prediction"
    targetGeneEnd: Option[Long],    // only for annotation_type == "target_gene_prediction"
    strand: Option[Strand]
  ) extends RenderableJsonRow {

  import Json.toJValue
  
  override def jsonValues: Seq[(String, JValue)] = Seq(
    "dataset" -> toJValue(dataset),
    "biosampleId" -> toJValue(biosampleId),
    "biosampleType" -> toJValue(biosampleType),
    "biosample" -> toJValue(biosample),
    "tissueId" -> toJValue(tissueId),
    "tissue" -> toJValue(tissue),
    "annotation" -> toJValue(annotation),
    "category" -> toJValue(category),
    "method" -> toJValue(method),
    "source" -> toJValue(source),
    "assay" -> toJValue(assay),
    "collection" -> toJValue(collection),
    "chromosome" -> toJValue(chromosome),
    "start" -> toJValue(start),
    "end" -> toJValue(end),
    "state" -> toJValue(state),
    "targetGene" -> toJValue(targetGene),
    "targetGeneStart" -> toJValue(targetGeneStart),
    "targetGeneEnd" -> toJValue(targetGeneEnd)
  )
}
