package loamstream.loam.intake.dga

import loamstream.loam.intake.RenderableRow

/**
 * @author clint
 * Jan 20, 2021
 */
final case class BedRow(
    biosampleId: String,    // e.g. UBERON:293289
    biosampleType: String,
    biosample: Option[String],
    tissueId: Option[String],   //TODO: Only Optional for now; e.g. UBERON:293289
    tissue: Option[String],
    annotation: String,    // annotation type, e.g. binding_site
    category: Option[String], //TODO: Only Optional for now
    method: Option[String],  //TODO: Only Optional for now; e.g. MAC2
    source: Option[String],   //TODO: Only Optional for now; e.g. ATAC-seq-peak
    assay: Option[String],   //TODO: Only Optional for now; e.g. ATAC-seq
    collection: Option[String], //TODO: Only Optional for now; e.g. ENCODE
    chromosome: String,
    start: Long,
    end: Long,
    state: String, //was name
    targetGene: Option[String],    // only for annotation_type == "target_gene_prediction"
    targetGeneStart: Option[Long],    // only for annotation_type == "target_gene_prediction"
    targetGeneEnd: Option[Long]    // only for annotation_type == "target_gene_prediction"
    /*chr: String,
    start: Int,
    end: Int,
    state: String,
    value: Double,
    strand: String,
    thickStart: Int,
    thickEnd: Int,
    itemRgb: String,
    blockCount: Int,
    blockSizes: Int,
    blockStarts: Int,
    chrom: Option[String],
    chromStart: Option[Int],
    chromEnd: Option[Int],
    name: Option[String],
    score: Option[Double]*/) extends RenderableRow {

  override def headers: Seq[String] = Seq(
    "biosampleId",
    "biosampleType",
    "biosample",
    "tissueId",
    "tissue",
    "annotation",
    "category",
    "method",
    "source",
    "assay",
    "collection",
    "chromosome",
    "start",
    "end",
    "state",
    "targetGene",
    "targetGeneStart",
    "targetGeneEnd")
  
  override def values: Seq[Option[String]] = Seq(
      Option(biosampleId),
      Option(biosampleType),
      biosample,
      tissueId,
      tissue,
      Option(annotation),
      category,
      method,
      source,
      assay,
      collection,
      Option(chromosome),
      Option(start.toString),
      Option(end.toString),
      Option(state),
      targetGene,
      targetGeneStart.map(_.toString),
      targetGeneEnd.map(_.toString))
}
