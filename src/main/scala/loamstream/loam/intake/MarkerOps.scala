package loamstream.loam.intake

/**
 * @author clint
 */
trait MarkerOps[R <: BaseVariantRow] {
  def transformMarker(row: R, f: Variant => Variant): R
}

object MarkerOps {
  implicit object PValueVariantRowMarkerOps extends MarkerOps[PValueVariantRow] {
    override def transformMarker(row: PValueVariantRow, f: Variant => Variant): PValueVariantRow = {
      row.copy(marker = f(row.marker))
    }
  }
  
  implicit object VariantCountRowMarkerOps extends MarkerOps[VariantCountRow] {
    override def transformMarker(row: VariantCountRow, f: Variant => Variant): VariantCountRow = {
      row.copy(marker = f(row.marker))
    }
  }
}