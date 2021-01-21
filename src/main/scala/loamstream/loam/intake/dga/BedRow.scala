package loamstream.loam.intake.dga

/**
 * @author clint
 * Jan 20, 2021
 */
final case class BedRow(
    chr: String,
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
    score: Option[Double]) {

}
