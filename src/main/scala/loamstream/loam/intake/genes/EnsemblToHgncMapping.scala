package loamstream.loam.intake.genes

import java.nio.file.Path
import loamstream.loam.intake.Source
import loamstream.loam.intake.ColumnExpr
import loamstream.loam.intake.ColumnName
import loamstream.loam.intake.DataRow
import org.apache.commons.csv.CSVFormat

/**
  * @author clint
  * @date Jul 20, 2021
  * 
  */
trait EnsemblToHgncMapping extends (EnsemblId => HgncId)

object EnsemblToHgncMapping {
  def fromFile(
    path: Path, 
    ensemblIdColumnName: String = "Gene stable ID",
    hgncIdColumnName: String = "Gene name",
    format: CSVFormat = Source.Formats.tabDelimitedWithHeader): EnsemblToHgncMapping = {

    new EnsemblToHgncMapping {
      private lazy val delegate: Map[EnsemblId, HgncId] = {
        val source = Source.fromFile(path, Source.Formats.tabDelimitedWithHeader)

        val ensemblIdExpr: ColumnExpr[EnsemblId] = ColumnName(ensemblIdColumnName).map(EnsemblId(_))
        val hgncIdExpr: ColumnExpr[HgncId] = ColumnName(hgncIdColumnName).map(HgncId(_))

        def toTuple(row: DataRow): (EnsemblId, HgncId) = ensemblIdExpr(row) -> hgncIdExpr(row)

        Map.empty ++ source.map(toTuple).records
      }

      override def apply(ensemblId: EnsemblId): HgncId = delegate(ensemblId)
    }
  }
}
